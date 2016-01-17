package io.stat.nabuproject.enki.leader;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.kafka.KafkaZKStringSerializerProxy;
import io.stat.nabuproject.core.net.AdvertisedAddressProvider;
import io.stat.nabuproject.core.util.dispatch.AsyncListenerDispatcher;
import io.stat.nabuproject.core.util.dispatch.ShutdownOnFailureCRC;
import io.stat.nabuproject.enki.Enki;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.Watcher;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.stat.nabuproject.core.util.functional.FluentCompositions.$;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.$_$;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.curry2;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.on;
import static io.stat.nabuproject.core.util.functional.FunMath.lcmp;
import static io.stat.nabuproject.core.util.functional.FunMath.lt;
import static io.stat.nabuproject.core.util.functional.FunMath.negate;
/**
 * A leader election implementation using ZooKeeper.
 * Due to the way the idiomatic ZooKeeper leader election strategy is
 * designed, this implementation will never quite report the "true leader". Rather, it will report
 * the node in front of it for ascension to leadership. Every a time an Enki reconnects to the
 * cluster, it will get put in the back of the line. Even if nabus round-robin their connections to
 * Enki, they will always send REDIRECT down the chain which will stop with whoever is the real leader.
 *
 * Needless to say it's a little inefficient. Furthermore, ZooKeeper doesn't care about "real time" so
 * much as "in order". Because of this, we have {@link io.stat.nabuproject.enki.integration.ESZKLeaderIntegrator}
 * which piggybacks off of this class, and integrates ElasticSearch cluster events which are near real-time to
 * <ol>
 *     <li>
 *         allow a more correct representation of the Nabu/Enki cluster state,
 *         especially if multiple nodes leave or join within the same timespan
 *     </li>
 *     <li>
 *         save the overhead of a huge redirect chain, especially if you use super-experimental software responsibly,
 *         and have 20 kubernetes-managed auto restarting Enkis as failovers.
 *     </li>
 * </ol>
 *
 * @see io.stat.nabuproject.enki.integration.ESZKLeaderIntegrator
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class ZKLeaderImpl extends EnkiLeaderElector implements ZKLeaderProvider {
    // todo: configurable?
    private static final String ELECTION_PATH = "/enki_le";
    private static final String ELECTION_PATH_SLASH = "/enki_le/";
    private static final String ELECTION_PREFIX = "n_";
    private static final String ELECTION_PATH_AND_PREFIX = ELECTION_PATH + "/" + ELECTION_PREFIX;

    private final ZKLeaderConfigProvider config;

    private final byte[] $leaderDataLock;

    private final AtomicBoolean amILeader;
    private final AtomicReference<LeaderData> electedLeaderData;

    private final AtomicLong myZNodeSequence;
    private final AtomicReference<String> myZNodePath;
    private final AtomicReference<ZKLeaderData> myLeaderData;

    private final AdvertisedAddressProvider addressProvider;
    private final ESClient esClient;

    private ZkClient zkClient;

    @Override
    public LeaderData getElectedLeaderData() {
        return electedLeaderData.get();
    }

    private final IZkChildListener electionPathChildListener;
    private final IZkStateListener connectionStateListener;

    private final AsyncListenerDispatcher<LeaderEventListener> dispatcher;

    /**
     * For shutting down the root Enki instance on callback failure.
     */
    private final Injector injector;

    @Inject
    public ZKLeaderImpl(ZKLeaderConfigProvider config,
                        ESClient esClient,
                        AdvertisedAddressProvider addressProvider,
                        Injector injector) {
        this.config = config;
        this.injector = injector;
        this.esClient = esClient;
        this.addressProvider = addressProvider;
        this.dispatcher = new AsyncListenerDispatcher<>("ZKLeaderEventDispatch");

        this.electionPathChildListener = new ElectionPathChildListener();
        this.connectionStateListener   = new ConnectionStateListener();

        this.$leaderDataLock = new byte[0];

        this.electedLeaderData = new AtomicReference<>(null);

        this.myZNodeSequence = new AtomicLong(-1);
        this.myZNodePath     = new AtomicReference<>("");
        this.myLeaderData    = new AtomicReference<>(null);
        this.amILeader       = new AtomicBoolean(false);


    }

    @Override
    public void start() throws ComponentException {
        Function<String, String> appendChroot = curry2($(String::concat), config.getLEZKChroot());
        try {
            Iterator<String> chrootedZookeepersIterator =
                    config.getLEZooKeepers()
                            .stream()
                            .map(appendChroot)
                            .iterator();

            this.zkClient = new ZkClient(
                    new ZkConnection(Joiner.on(',').join(chrootedZookeepersIterator)),
                    config.getLEZKConnTimeout(),
                    new KafkaZKStringSerializerProxy());

            createLeaderData();

            synchronized ($leaderDataLock) {
                try {
                    // bootstrap the first connection
                    electionPathChildListener.handleChildChange(null, ImmutableList.of(myZNodePath.get().replaceFirst(ELECTION_PATH + "/", "")));
                } catch(Exception e) {
                    throw new ComponentException(true, "Couldn't register the ZK child node change listener!", e);
                }
            }
        } catch(Exception e) {
            if(e instanceof ComponentException) {
                throw e;
            } else {
                throw new ComponentException(true, "Failed to start the ZK leader election system", e);
            }
        }
    }

    private ZKLeaderData getOrCreateOwnNode() {
        synchronized ($leaderDataLock) {
            try {
                // make sure our own node still exists!
                ZKLeaderData ownLookedUpLEData = getFromFullPath(myZNodePath.get());
                if (!myLeaderData.get().equals(ownLookedUpLEData)) {
                    logger.error("FATAL: My leader election data is gone or mismatched from ZK.");
                    haltAndCatchFire();
                }

                return ownLookedUpLEData;
            } catch (ZkNoNodeException zknne) {
                logger.warn("Received a ZkNoNodeException while attempting to read my own data... recreating leader node", zknne);
                return createLeaderData();
            }
        }
    }

    // retries for createLeaderData, to prevent
    // and infinite loop when we go back to sanity check.
    private static volatile int cLDRetries = 0;
    private ZKLeaderData createLeaderData() {
        synchronized ($leaderDataLock){
            cLDRetries++;

            if(cLDRetries >= 5) {
                haltAndCatchFire();
            }

            // pause events for a little bit.
            zkClient.unsubscribeAll();

            // One important thing to remember is that the path
            // and the priority (which we cant since we dont know it,
            // and the priority is just the sequence id ZK assigns to use)
            // don't actually get serialized to ZK and when other nodes read
            // the data they fill in the path for themselves
            // so it's perfectly OK to do this, as long as we remember to update
            // our own internal data!
            ZKLeaderData seed = new ZKLeaderData(
                    "<path not yet determined>",
                    esClient.getESIdentifier(),
                    addressProvider.getAdvertisedAddress(),
                    0 // seeded at 0, but value is updated after the node has been committed.
            );

            String zkEncodedSeed = seed.toBase64();
            String newZnodePath = zkClient.createEphemeralSequential(ELECTION_PATH_AND_PREFIX, zkEncodedSeed);
            long newSequence = parseFromPathAndPrefix(newZnodePath);

            seed = new ZKLeaderData(
                    newZnodePath,
                    seed.getNodeIdentifier(),
                    seed.getAddressPort(),
                    newSequence
            );

            myLeaderData.set(seed);
            myZNodePath.set(newZnodePath);
            myZNodeSequence.set(newSequence);

            logger.info("created leader election ephemeral + sequential znode at {}{} with {}",
                    config.getLEZKChroot(), newZnodePath, zkEncodedSeed);

            zkClient.subscribeStateChanges(connectionStateListener);
            zkClient.subscribeChildChanges(ELECTION_PATH, electionPathChildListener);

            // for the sake of sanity, pull our own data back.
            ZKLeaderData rePulledData = getOrCreateOwnNode();

            // if we got this far we can re-set cLDRetries
            cLDRetries = 0;
            return rePulledData;
        }
    }

    @Override
    public void shutdown() throws ComponentException {
        // todo: stop watchers, etc.
        // may not actually be necessary...
        if(this.zkClient != null) {
            this.zkClient.close();
        }

        this.dispatcher.shutdown();
    }

    @Override
    public boolean isSelf() {
        synchronized($leaderDataLock) {
            return amILeader.get();
        }
    }

    @Override
    public LeaderData getOwnLeaderData() { return myLeaderData.get(); }

    @Override
    public List<LeaderData> getLeaderCandidates() {
        return getLeaderCandidatesFromSubnodeList(getElectionSubnodes());
    }

    private List<LeaderData> getLeaderCandidatesFromSubnodeList(List<String> electionSubnodes) {
        return electionSubnodes.stream().map(this::getFromChildNodeName).collect(Collectors.toList());
    }

    private List<String> getElectionSubnodes() {
        return zkClient.getChildren(ELECTION_PATH);
    }

    private ZKLeaderData getFromChildNodeName(String child) {
        long seq = parseChildSequence(child);
        return zkldFromZkPath(ELECTION_PATH_SLASH + child, seq);
    }

    private ZKLeaderData getFromFullPath(String path) {
        long seq = parseFromPathAndPrefix(path);
        return zkldFromZkPath(path, seq);
    }

    private ZKLeaderData zkldFromZkPath(String path, long seq) {
        return ZKLeaderData.fromBase64(zkClient.readData(path), path, seq);
    }

    private static long parseFromPathAndPrefix(String nodePath) {
        return Long.parseLong(nodePath.replaceFirst(ELECTION_PATH_AND_PREFIX, ""), 10);
    }
    private static long parseChildSequence(String child) {
        return Long.parseLong(child.replaceFirst(ELECTION_PREFIX, ""), 10);
    }

    private void setLeader(boolean isSelf, LeaderData leader) {
        synchronized ($leaderDataLock) {
            amILeader.set(isSelf);
            electedLeaderData.set(leader);
        }

        dispatchLeaderChange(isSelf);
    }

    private void dispatchLeaderChange(boolean isSelf) {
        dispatcher.dispatchListenerCallbacks(l -> l.onLeaderChange(isSelf, electedLeaderData.get(), getLeaderCandidates()),
                new ShutdownOnFailureCRC(injector.getInstance(Enki.class),
                        "ZKLE" + (isSelf ? "Self" : "Other") +"ElectedCallbackFailed"));
    }

    @Override
    public void addLeaderEventListener(LeaderEventListener listener) {
        synchronized ($leaderDataLock) {
            dispatcher.addListener(listener);
            if(!listener.onLeaderChange(amILeader.get(), getElectedLeaderData(), getLeaderCandidates())) {
                // kill misbehaving listeners before they even become a problem,
                // as well as seed them.
                injector.getInstance(Enki.class).shutdown();
            }
        }
    }

    @Override
    public void removeLeaderEventListener(LeaderEventListener listener) {
        dispatcher.removeListener(listener);
    }

    private void haltAndCatchFire() {
        // needs to be a new thread because otherwise it may be inside
        // ZK's or some other component's thread pool.
        Thread hcf = new Thread(() -> {
            logger.error("FATAL: Shutting down Enki.");
            zkClient.unsubscribeAll();
            zkClient.close();
            this.shutdown();
            injector.getInstance(Enki.class).shutdown();
        });
        hcf.setName("ZKLeader-HCF");
        hcf.start();
    }

    private final class ElectionPathChildListener implements IZkChildListener {
        final Function<String, ZKLeaderData> ZKLDLoader = ZKLeaderImpl.this::getFromChildNodeName;

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            synchronized ($leaderDataLock) {
                ZKLeaderData myRefreshedData = getOrCreateOwnNode();
                if(!myLeaderData.get().equals(myRefreshedData)) {
                    logger.error("FATAL: mismatch in the data that's cached in the ZKLeaderImpl and what's been pulled from ZK.");
                    haltAndCatchFire();
                }

                doChildChange(currentChilds, myRefreshedData);
            }
        }

        public void doChildChange(List<String> currentChilds, ZKLeaderData myRefreshedData) {
            long ownZnodeSeq = myRefreshedData.getPriority();
            Predicate<ZKLeaderData> previousLeaders = $         (ZKLeaderData::isAcceptable)
            /*  ~~ from keith bankaccounts with love ~~ */ .and(
                                  $_$                           (ZKLeaderData::getPriority, lt(ownZnodeSeq)));


            ZKLeaderData highestLeaderBeforeMe =
                    currentChilds
                            .stream()
                            .map(ZKLDLoader)
                            .filter(previousLeaders)
                            .sorted(on(ZKLeaderData::getPriority, lcmp).then(negate))
                            .findFirst()
                            .orElse(myLeaderData.get());


            boolean amILeader = highestLeaderBeforeMe.equals(myLeaderData.get());

            if (amILeader) {
                logger.info("A change in the leader election ZNode has been detected, and I am the leader. " +
                        "({})",highestLeaderBeforeMe.prettyPrint());
            } else {
                logger.info("A change in the leader election path has been detected, and I am NOT the leader. " +
                        "The node before me is {}", highestLeaderBeforeMe.prettyPrint());
            }

            setLeader(amILeader, highestLeaderBeforeMe);
        }
    }

    private final class ConnectionStateListener implements IZkStateListener, Runnable {
        @Override
        public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
            if(keeperState.equals(Watcher.Event.KeeperState.Disconnected)
                    || keeperState.equals(Watcher.Event.KeeperState.Expired)) {
                Thread t = new Thread(this);
                t.setName("ZKLeaderDisconnectedShutdownThread");
                t.start();
            }
        }

        @Override
        public void handleNewSession() throws Exception { /* no-op */ }

        @Override
        public void handleSessionEstablishmentError(Throwable error) throws Exception {
            logger.error("[FATAL] Session establishment error", error);
            haltAndCatchFire();
        }

        /**
         * Performs the full enki shutdown from outside the ZK Event loop
         * (since the ZK client will get stopped in the case of a disconnect)
         */
        @Override
        public void run() {
            logger.error("ZkConnectionState changed to DISCONNECTED. Shutting down Enki");
            haltAndCatchFire();
        }
    }
}
