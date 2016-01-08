package io.stat.nabuproject.enki.leader;

import com.google.common.base.Joiner;
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
 *
 * todo: Needs to be able to integrate with ElasticSearch cluster events
 *       since there is a bit of a delay between ZooKeeper watch events
 *       and actual node events.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class ZKLeaderImpl extends EnkiLeaderElector implements ZKLeaderProvider {
    // todo: configurable?
    private static final String ELECTION_PATH = "/enki_le";
    private static final String ELECTION_PREFIX = "n_";
    private static final String FULL_ELECTION_PREFIX = ELECTION_PATH + "/" + ELECTION_PREFIX;

    private final ZKLeaderConfigProvider config;

    private ZKLeaderData myLeaderData;

    private final byte[] $leaderDataLock;
    private final AtomicBoolean isLeader;
    private final AtomicReference<LeaderData> electedLeaderData;

    private final AtomicLong ownZNodeSequence;
    private final AtomicReference<String> ownZNodePath;

    private ZkClient zkClient;

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

        this.$leaderDataLock = new byte[0];
        this.isLeader = new AtomicBoolean(false);
        this.electedLeaderData = new AtomicReference<>(null);

        this.ownZNodeSequence = new AtomicLong(-1);
        this.ownZNodePath     = new AtomicReference<>("");
        this.myLeaderData =  new ZKLeaderData(
                "<path not yet determined>",
                esClient.getESIdentifier(),
                addressProvider.getAdvertisedAddress(),
                0 // seeded at 0, but value is updated after the node has been committed.
        );

        this.electionPathChildListener = new ElectionPathChildListener();
        this.connectionStateListener   = new ConnectionStateListener();

        this.dispatcher = new AsyncListenerDispatcher<>("ZKLeaderEventDispatch");
    }

    @Override
    public void start() throws ComponentException {
        try {
            Iterator<String> chrootedZookeepersIterator =
                    config.getLEZooKeepers()
                            .stream()
                            .map(zk -> zk + config.getLEZKChroot())
                            .iterator();

            this.zkClient = new ZkClient(
                    new ZkConnection(Joiner.on(',').join(chrootedZookeepersIterator)),
                    config.getLEZKConnTimeout(),
                    new KafkaZKStringSerializerProxy());

            String mldb64 = myLeaderData.toBase64();
            String ownLEZNodePath = zkClient.createEphemeralSequential(FULL_ELECTION_PREFIX, mldb64);
            logger.info("created leader election ephemeral + sequential znode at {}{} with {}",
                    config.getLEZKChroot(), ownLEZNodePath, mldb64);

            zkClient.subscribeStateChanges(connectionStateListener);

            ownZNodeSequence.set(parseFullSequence(ownLEZNodePath));
            ownZNodePath.set(ownLEZNodePath);

            // now that we know our own ZNode sequence, update myLeaderData!
            myLeaderData = new ZKLeaderData(
                    ownLEZNodePath,
                    myLeaderData.getNodeIdentifier(),
                    myLeaderData.getAddressPort(),
                    ownZNodeSequence.get()
            );

            List<String> children = zkClient.subscribeChildChanges(ELECTION_PATH, electionPathChildListener);

            synchronized ($leaderDataLock) {
                try {
                    // bootstrap the first connection
                    electionPathChildListener.handleChildChange(null, children);
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
            return isLeader.get();
        }
    }

    @Override
    public LeaderData getOwnLeaderData() { return myLeaderData; }

    @Override
    public List<LeaderData> getLeaderCandidates() {
        return getLeaderCandidatesFromSubnodeList(getElectionSubnodes());
    }

    private List<LeaderData> getLeaderCandidatesFromSubnodeList(List<String> electionSubnodes) {
        return electionSubnodes.stream().map(this::getFromChild).collect(Collectors.toList());
    }

    private List<String> getElectionSubnodes() {
        return zkClient.getChildren(ELECTION_PATH);
    }

    private ZKLeaderData getFromChild(String child) {
        long seq = parseChildSequence(child);
        return zkldFromZkPath(ELECTION_PATH + "/" + child, seq);
    }

    private ZKLeaderData getFromPath(String path) {
        long seq = parseFullSequence(path);
        return zkldFromZkPath(path, seq);
    }

    private ZKLeaderData zkldFromZkPath(String path, long seq) {
        return ZKLeaderData.fromBase64(zkClient.readData(path), path, seq);
    }

    private static long parseFullSequence(String nodePath) {
        return Long.parseLong(nodePath.replaceFirst(FULL_ELECTION_PREFIX, ""), 10);
    }
    private static long parseChildSequence(String child) {
        return Long.parseLong(child.replaceFirst(ELECTION_PREFIX, ""), 10);
    }

    private void setLeader(boolean isSelf, LeaderData leader) {
        synchronized ($leaderDataLock) {
            isLeader.set(isSelf);
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
            if(!listener.onLeaderChange(isLeader.get(), myLeaderData, getLeaderCandidates())) {
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
        final Function<String, ZKLeaderData> ZKLDLoader = ZKLeaderImpl.this::getFromChild;

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            synchronized ($leaderDataLock) {

                try {
                    // make sure our own node still exists!
                    ZKLeaderData ownLookedUpLEData = getFromPath(ownZNodePath.get());
                    if(!myLeaderData.equals(ownLookedUpLEData)) {
                        logger.error("FATAL: My leader election data is gone or mismatched from ZK.");
                        haltAndCatchFire();
                    }
                } catch(Exception e) {
                    logger.error("FATAL: Got an Exception while trying to look up my ZK data", e);
                    haltAndCatchFire();
                }

                Long ownZnodeSeq = ownZNodeSequence.get();
                Predicate<ZKLeaderData> previousLeaders =    $(ZKLeaderData::isAcceptable)
                                                         .and(
                                                           $_$(ZKLeaderData::getPriority, curry2(lt, ownZnodeSeq)));
                ZKLeaderData highestLeaderBeforeMe =
                    currentChilds
                        .stream()
                        .map(ZKLDLoader)
                        .filter(previousLeaders)
                        .sorted(on(ZKLeaderData::getPriority, lcmp).then(negate))
                        .findFirst()
                        .orElse(myLeaderData);


                boolean amILeader = highestLeaderBeforeMe.equals(myLeaderData);

                if (amILeader) {
                    logger.info("A change in the leader election ZNode has been detected, and I am the leader. " +
                            "({})", myLeaderData.prettyPrint());
                } else {
                    logger.info("A change in the leader election path has been detected, and I am NOT the leader. " +
                            "The node before me is {}", highestLeaderBeforeMe.prettyPrint());
                }

                setLeader(amILeader, highestLeaderBeforeMe);
            }
        }
    }

    private final class ConnectionStateListener implements IZkStateListener, Runnable {
        @Override
        public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
            if(keeperState == Watcher.Event.KeeperState.Disconnected) {
                Thread t = new Thread(this);
                t.setName("ZKLeaderDisconnectedShutdownThread");
                t.start();
            }
        }

        @Override
        public void handleNewSession() throws Exception { /* no-op */ }

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
