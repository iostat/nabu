package io.stat.nabuproject.enki.leader;

import com.google.common.base.Joiner;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.net.AdvertisedAddressProvider;
import io.stat.nabuproject.core.util.Tuple;
import io.stat.nabuproject.core.util.dispatch.AsyncListenerDispatcher;
import io.stat.nabuproject.core.util.dispatch.ShutdownOnFailureCRC;
import io.stat.nabuproject.enki.Enki;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    private final AdvertisedAddressProvider advertisedAddressProvider;

    private final byte[] $leaderDataLock;
    private final AtomicBoolean isLeader;
    private final AtomicReference<String> leaderAddress;
    private final AtomicInteger leaderPort;

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
                        AdvertisedAddressProvider addressProvider,
                        Injector injector) {
        this.config = config;
        this.advertisedAddressProvider = addressProvider;
        this.injector = injector;

        this.$leaderDataLock = new byte[0];
        this.isLeader = new AtomicBoolean(false);
        this.leaderAddress = new AtomicReference<>("");
        this.leaderPort = new AtomicInteger(-1);

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
                    Joiner.on(',').join(chrootedZookeepersIterator),
                    config.getLEZKConnTimeout());

            String collatedData = collateZNodeData();
            String ownLEZNodePath = zkClient.createEphemeralSequential(FULL_ELECTION_PREFIX, collatedData);
            logger.info("created leader election ephemeral + sequential znode at \"{}{}\" with data \"{}\"",
                    config.getLEZKChroot(), ownLEZNodePath, collatedData);

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
    public String getLeaderAddress() {
        synchronized ($leaderDataLock) {
            return leaderAddress.get();
        }
    }

    @Override
    public int getLeaderPort() {
        synchronized ($leaderDataLock) {
            return leaderPort.get();
        }
    }

    private String collateZNodeData() {
        AddressPort ap = advertisedAddressProvider.getAdvertisedAddress();
        return ap.getAddress() + "|" + ap.getPort();
    }

    private String getAddressFromLeaderData(String data) {
        return data.split("|")[0];
    }

    private int getPortFromLeaderData(String data) {
        return Integer.parseInt(data.split("|")[1]);
    }

    private static long parseSequence(String nodePath) {
        return Integer.parseInt(nodePath.replaceFirst(FULL_ELECTION_PREFIX, ""), 10);
    }

    private void setSelfAsLeader() {
        synchronized ($leaderDataLock) {
            if(isLeader.get()) {
                return;
            }

            AddressPort ap = advertisedAddressProvider.getAdvertisedAddress();

            isLeader.set(true);
            leaderAddress.set(ap.getAddress());
            leaderPort.set(ap.getPort());
        }

        dispatcher.dispatchListenerCallbacks(LeaderEventListener::onSelfElected,
                new ShutdownOnFailureCRC(injector.getInstance(Enki.class),
                        "ZKLESelfElectedCallbackFailed"));
    }

    private void setOtherAsLeader(String leaderData) {
        String newAddress = getAddressFromLeaderData(leaderData);
        int    newPort    = getPortFromLeaderData(leaderData);

        synchronized ($leaderDataLock) {
            isLeader.set(false);
            leaderAddress.set(newAddress);
            leaderPort.set(newPort);
        }

        dispatcher.dispatchListenerCallbacks(listener -> listener.onOtherElected(newAddress, newPort),
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

    private final class ElectionPathChildListener implements IZkChildListener {
        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            synchronized ($leaderDataLock) {
                long own = ownZNodeSequence.get();
                Tuple<String, Long> highestSequenceUpToOwn = currentChilds.stream()
                        .filter(child -> !child.startsWith("/")) // dont look at new paths or w/e
                        .map(child -> new Tuple<>(child, Long.parseLong(child.replaceFirst(ELECTION_PREFIX, ""))))
                        .filter(candidate -> candidate.second() < own)
                        .sorted((a, b) -> Longs.compare(a.second(), b.second()) * -1) // reverse sort
                        .findFirst()
                        .orElse(new Tuple<>("irrelevant", own));

                if (highestSequenceUpToOwn.second() == own) {
                    logger.info("A change in the leader election path has been detected, and I am the leader.");
                    setSelfAsLeader();
                } else {
                    String leaderData = zkClient.readData(ELECTION_PATH + "/" + highestSequenceUpToOwn.first());
                    logger.info("A change in the leader election path has been detected, and I am NOT the leader. The leader is {}", leaderData);
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
