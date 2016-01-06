package io.stat.nabuproject.enki.leader;

import com.google.common.base.Joiner;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.kafka.KafkaZKStringSerializerProxy;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.net.AdvertisedAddressProvider;
import io.stat.nabuproject.core.util.Tuple;
import io.stat.nabuproject.core.util.dispatch.AsyncListenerDispatcher;
import io.stat.nabuproject.core.util.dispatch.ShutdownOnFailureCRC;
import io.stat.nabuproject.core.util.functional.FunMath;
import io.stat.nabuproject.enki.Enki;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.Watcher;
import org.elasticsearch.common.Strings;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.stat.nabuproject.core.util.functional.FluentCompositions.compose;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.composep;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.curry2p;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.on;

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
class ZKLeaderImpl extends EnkiLeaderElector {
    // todo: configurable?
    private static final String ELECTION_PATH = "/enki_le";
    private static final String ELECTION_PREFIX = "n_";
    private static final String FULL_ELECTION_PREFIX = ELECTION_PATH + "/" + ELECTION_PREFIX;

    private final ZKLeaderConfigProvider config;
    private final ESClient esClient;
    private final AdvertisedAddressProvider advertisedAddressProvider;

    private final ZKLeaderData myLeaderData;

    private final byte[] $leaderDataLock;
    private final AtomicBoolean isLeader;
    private final AtomicReference<ZKLeaderData> electedLeaderData;

    private final AtomicLong ownZNodeSequence;

    private ZkClient zkClient;

    private final IZkChildListener electionPathChildListener;
    private final IZkStateListener connectionStateListener;

    private final AsyncListenerDispatcher<LeaderEventListener> dispatcher;

    /**
     * For grabbing the root Enki instance.
     */
    private final Injector injector;

    @Inject
    public ZKLeaderImpl(ZKLeaderConfigProvider config,
                        ESClient esClient,
                        AdvertisedAddressProvider addressProvider,
                        Injector injector) {
        this.config = config;
        this.esClient = esClient;
        this.advertisedAddressProvider = addressProvider;
        this.injector = injector;

        this.$leaderDataLock = new byte[0];
        this.isLeader = new AtomicBoolean(false);
        this.electedLeaderData = new AtomicReference<>(null);

        this.myLeaderData =  new ZKLeaderData(
                esClient.getElasticSearchIndentifier(),
                advertisedAddressProvider.getAdvertisedAddress()
        );

        this.ownZNodeSequence = new AtomicLong(-1);

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

            ownZNodeSequence.set(parseSequence(ownLEZNodePath));
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
    public AddressPort getElectedLeaderAP() {
        return electedLeaderData.get().getAddressPort();
    }

    private String getAddressFromLeaderData(String data) {
        return data.split("\\|")[0];
    }

    private int getPortFromLeaderData(String data) {
        return Integer.parseInt(data.split("\\|")[1]);
    }

    private static long parseSequence(String nodePath) {
        return Integer.parseInt(nodePath.replaceFirst(FULL_ELECTION_PREFIX, ""), 10);
    }

    private void setSelfAsLeader() {
        synchronized ($leaderDataLock) {
            if(isLeader.get()) {
                return;
            }

            isLeader.set(true);
            electedLeaderData.set(myLeaderData);
        }

        dispatcher.dispatchListenerCallbacks(LeaderEventListener::onSelfElected,
                new ShutdownOnFailureCRC(injector.getInstance(Enki.class),
                        "ZKLESelfElectedCallbackFailed"));
    }

    private void setOtherAsLeader(ZKLeaderData newLeader) {
        synchronized ($leaderDataLock) {
            isLeader.set(false);
            electedLeaderData.set(newLeader);
        }

        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onOtherElected(newLeader.getNodeIdentifier(), newLeader.getAddressPort()),
                new ShutdownOnFailureCRC(injector.getInstance(Enki.class),
                        "ZKLEOnOtherElectedCallbackFailed"));
    }

    @Override
    public void addLeaderEventListener(LeaderEventListener listener) {
        dispatcher.addListener(listener);
    }

    @Override
    public void removeLeaderEventListener(LeaderEventListener listener) {
        dispatcher.removeListener(listener);
    }

    @Override
    public boolean isLeader() {
        return false;
    }

    // used internally by ElectionPathChildListener to keep a tuple of a nodes pathname and the number within it
    // (pathname tuple)
    private static class PNTuple extends Tuple<String, Long> {
        PNTuple(String s) { super(s, Long.parseLong(s.replaceFirst(ELECTION_PREFIX, ""))); }
        PNTuple(String s, long l) { super(s, l); }
    }

    private final class ElectionPathChildListener implements IZkChildListener {
        private ZKLeaderData getFromChild(String child) {
            return ZKLeaderData.fromBase64(zkClient.readData(ELECTION_PATH + "/" + child));
        }

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            synchronized ($leaderDataLock) {
                long own = ownZNodeSequence.get();
                PNTuple highestSequenceUpToOwn = currentChilds.stream()
                        .filter(composep(this::getFromChild, ZKLeaderData::isAcceptable))   // child  -> getFromChild(child).isAcceptable()
                        .map(PNTuple::new)                                                  // child  -> new PNTuple(child)
                        .filter(composep(PNTuple::second, curry2p(FunMath.lt, own)))        // tuple  -> tuple.second() < own
                        .sorted(compose(on(PNTuple::second, Longs::compare), FunMath.neg))  // (a, b) -> Longs.compare(a.second(), b.second()) * -1;
                        .findFirst().orElse(new PNTuple("irrelevant", own));

                if (highestSequenceUpToOwn.second() == own) {
                    logger.info("A change in the leader election ZNode has been detected, and I am the leader. " +
                            "(n_{})", Strings.padStart(Long.toString(own), 10, '0'));
                    setSelfAsLeader();
                } else {
                    ZKLeaderData leaderData =  getFromChild(highestSequenceUpToOwn.first());
                    logger.info("A change in the leader election path has been detected, and I am NOT the leader. " +
                            "The node before me is {} => {}", highestSequenceUpToOwn.first(), leaderData);
                    setOtherAsLeader(leaderData);
                }
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
            zkClient.unsubscribeAll();
            zkClient.close();
            ZKLeaderImpl.this.shutdown();
            injector.getInstance(Enki.class).shutdown();
        }
    }
}
