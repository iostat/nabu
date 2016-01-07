package io.stat.nabuproject.enki.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEvent;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEventListener;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import io.stat.nabuproject.core.util.Tuple;
import io.stat.nabuproject.enki.leader.ElectedLeaderProvider;
import io.stat.nabuproject.enki.leader.LeaderData;
import io.stat.nabuproject.enki.leader.LeaderEventListener;
import io.stat.nabuproject.enki.leader.ZKLeaderProvider;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.stat.nabuproject.core.util.functional.FluentCompositions.on;

/**
 * Integrates ElasticSearch cluster state and ZooKeeper leader election events
 * to provide a more reliable/current leader state. The implmentations for
 * {@link ElectedLeaderProvider} will all block until reliable data is known.
 *
 * todo: there are probably a TON of redundant operations which can be optimized to hell and back
 * but I don't really care for it at the moment. better safe than sorry right?
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class ESZKLeaderIntegrator extends LeaderLivenessIntegrator implements LeaderEventListener, NabuESEventListener {
    // todo: should these be tunable?
    /**
     * Amount of time to sleep before and after each read to allow write operations to happen
     * before data is returned.
     * The idea being, this reduces the risk of a race condition leading to stale data wherein nothing has updated
     * the integrated leader state prior to a read lock being obtained, or it was updated shortly after the read
     * finished.
     */
    public static final int SETTLE_DELAY = 100;

    // How long to wait before retrying ZK reconciliation queue
    // entries if there's no match yet.
    public static final int ZK_RECONCILE_DELAY = 500;

    /**
     * How to long to wait before retrying finding
     * a leader while one is not available.
     */
    public static final int NO_LEADER_RETRY_DELAY = 200;

    /**
     * Maximum amount of times to retry getting a leader before giving up
     */
    public static final int MAX_LEADER_RETRIES = 10;

    private final ESClient esClient;
    private final ZKLeaderProvider zkLeaderProvider;
    private final StampedLock $integratedDataLock;
    private final Set<ESZKNode> integratedData;

    private final ThreadPoolExecutor zkReconciler;
    private final StampedLock $reconcileQueueLock;
    private final Set<String> reconcileQueue;

    public static Predicate<LeaderData> existsInES(List<Tuple<String, AddressPort>> knownFromES) {
        return l -> knownFromES.stream().anyMatch(t -> t.first().equals(l.getNodeIdentifier()));
    }

    @Inject
    ESZKLeaderIntegrator(ESClient esClient, ZKLeaderProvider zkLeaderProvider) {
        this.esClient = esClient;
        this.zkLeaderProvider = zkLeaderProvider;
        this.$integratedDataLock = new StampedLock();

        this.integratedData = Sets.newConcurrentHashSet();

        this.zkReconciler = new ThreadPoolExecutor(
                1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(20),
                new NamedThreadFactory("ZKLeaderReconciler")
        );
        this.$reconcileQueueLock = new StampedLock();
        this.reconcileQueue = Sets.newConcurrentHashSet();

        logger.info("ESZKLeaderIntegrator constructed!");
    }

    @Override
    public void start() throws ComponentException {
        logger.info("ESZKLeaderIntegrator called start()!");
        performWrite(() -> {
            integratedData.clear();

            Iterator<Tuple<String, AddressPort>> tupIt = esClient.getDiscoveredEnkis().iterator();
            // don't start the reconciler until the ES thing has been seeded.
            for(; tupIt.hasNext();) {
                String nodeName = tupIt.next().first();
                integratedData.add(new ESZKNode(nodeName, null, false));
                scheduleZKReconciler(nodeName, false);
            }
        }
        , $integratedDataLock);

        // start the initial reconciliation.
        zkReconciler.submit(this::reconcileZk);

        esClient.addNabuESEventListener(this);
        zkLeaderProvider.addLeaderEventListener(this);

        logger.info("ESZKLeaderIntegrator started!");
    }

    @Override
    public void shutdown() throws ComponentException {

        zkLeaderProvider.removeLeaderEventListener(this);
        esClient.removeNabuESEventListener(this);

        zkReconciler.shutdown();

        performWrite(reconcileQueue::clear, $reconcileQueueLock);
        performWrite(integratedData::clear, $integratedDataLock);

        logger.info("ESZKLeaderIntegrator stopped!");
    }

    @Override
    public boolean isSelf() {
        logger.info("ESZKLeaderIntegrator#isSelf()");
        Tuple<Boolean, ESZKNode> tuple = validLeaderIfAvailable();
        return (tuple.first() && tuple.second().getLeaderData().equals(getOwnLeaderData()));
    }

    @Override
    public List<LeaderData> getLeaderCandidates() {
        List<LeaderData> resultOfRead = optimisticRead(() ->
                ImmutableList.copyOf(
                        integratedData.stream()
                                      .filter(ESZKNode::hasLeaderData)
                                      .map(ESZKNode::getLeaderData)
                                      .iterator())
        , $integratedDataLock);

        return resultOfRead == null ? ImmutableList.of() : resultOfRead;
    }

    @Override
    public LeaderData getOwnLeaderData() {
        logger.info("ESZKLeaderIntegrator#getOwnLeaderData()");
        return zkLeaderProvider.getOwnLeaderData();
    }

    @Override @SneakyThrows
    public LeaderData getElectedLeaderData() {
        logger.info("ESZKLeaderIntegrator#getElectedLeaderData()");
        LeaderData ret = getOwnLeaderData();
        int tries = 0;
        while(tries < MAX_LEADER_RETRIES) {
            Tuple<Boolean, ESZKNode> maybe = validLeaderIfAvailable();
            if(maybe.first()) {
                return maybe.second().getLeaderData();
            }
            Thread.sleep(NO_LEADER_RETRY_DELAY);
            tries++;
        }

        Set<ESZKNode> copy = optimisticRead(() -> ImmutableSet.copyOf(integratedData), $integratedDataLock);
        logger.error("Couldn't find a valid leader after {} tries... Sending self as leader, but this may be VERY wrong\n" +
                "My most current data is: ", MAX_LEADER_RETRIES);

        return ret;
    }

    /**
     * Finds the node with the lowest "priority"
     * which coincidentally is the ZNode number in the only
     * implementation we have
     * @param allLeaders a list of all potential leaders ZK is aware of
     * @return a leader, or null.
     */
    private LeaderData findTrueLeader(List<LeaderData> allLeaders) {
        logger.info("ESZKLeaderIntegrator#findTrueLeader()");
        List<Tuple<String, AddressPort>> knownEsNodes = esClient.getDiscoveredEnkis();
        return allLeaders.stream()
                         .filter(existsInES(knownEsNodes))
                         .sorted(on(LeaderData::getPriority, Longs::compare))
                         .findFirst()
                         .orElse(null);
    }

    /**
     * Called by ZK if there's any activity in the leader election ZNode
     * @param isSelf whether or not this node is the new leader.
     * @param myData the LeaderData of the new leader
     * @param allLeaderData the leaderDatas of all the nodes.
     * @return always true
     */
    @Override
    public boolean onLeaderChange(boolean isSelf, LeaderData myData, List<LeaderData> allLeaderData) {
        logger.info("ESZKLeaderIntegrator#onLeaderChange({}, {}, {})", isSelf, myData, allLeaderData);
        LeaderData trueLeader = findTrueLeader(allLeaderData);
        if(isSelf) {
            logger.info("This node is allegedly the leader, and my check says its: {}\nThis: {}\nThat: {}", trueLeader.equals(myData), myData, trueLeader);
            setAsLeader(myData);
        } else {
            logger.info("Someone else is allegedly the leader, and my check says its: {}\nThis: {}\nThat: {}", !trueLeader.equals(myData), myData, trueLeader);
            setAsLeader(trueLeader);
        }

        return true;
    }

    @Override
    public void onNabuESEvent(NabuESEvent event) {
        String nodeName = event.getNode().getName();
        if(event.getType() == NabuESEvent.Type.ENKI_JOINED) {
            logger.info("New Enki joined! ({})", nodeName);
            registerEsNode(nodeName, true, true);
        } else if (event.getType() == NabuESEvent.Type.ENKI_PARTED) {
            logger.info("Enki {} departed ES cluster.", nodeName);
            performWrite(() ->
                    reconcileQueue.removeIf(nn -> nn.equals(nodeName))
            , $reconcileQueueLock);
            performWrite(() ->
                    integratedData.removeIf(node -> node.getEsNodeName().equals(nodeName))
            , $integratedDataLock);

            Tuple<Boolean, ESZKNode> test = validLeaderIfAvailable();
            logger.info("Immediate call to validLeaderAvailable returned: {}\n" +
                    "If the node that just left was the leader, the result should be <false, null>," +
                    "unless there was a ZK change event fired immediately before this test.\n" +
                    "If it was not the leader or no ZK event happened, tell a police officer or an MTA employee.", test);
        }
    }

    private void registerEsNode(String nodeName, boolean scheduleReconciler, boolean startReconciler) {
        logger.info("ESZKLeaderIntegrator#registerEsNode({}, {}, {})", nodeName, scheduleReconciler, startReconciler);
        performWrite(() ->
                integratedData.add(new ESZKNode(nodeName, null, false))
        , $integratedDataLock);

        if(scheduleReconciler) {
            scheduleZKReconciler(nodeName, startReconciler);
        }
    }

    private void setAsLeader(LeaderData ld) {
        performWrite(() -> {
            boolean seen = false;
            for(ESZKNode inode : integratedData) {
                if(inode.getLeaderData() == null) {
                    // could have just joined, ES added a node with no LD,
                    // but the names match.
                    if(inode.getEsNodeName().equals(ld.getNodeIdentifier())) {
                        inode.setLeaderData(ld);
                        inode.setLeader(true);
                        seen = true;
                        logger.info("Reconciled ESZK link for {}", inode);
                    }
                } else {
                    if (inode.getLeaderData().equals(ld)) {
                        inode.setLeader(true);
                        seen = true;
                    } else {
                        inode.setLeader(false);
                    }
                }
            }

            if(!seen) {
                logger.warn("Could not find any integrated ESZK data for {}", ld);
                if(esClient.getDiscoveredEnkis()
                           .stream()
                           .anyMatch(t ->
                                     t. first().equals(ld.getNodeIdentifier())
                                  && t.second().equals(ld.getAddressPort()))) {

                    ESZKNode newInode = new ESZKNode(ld.getNodeIdentifier(), ld, true);
                    logger.info("Found {} in the ES cluster!, Generated new ESZK link for: {}", ld.getNodeIdentifier(), newInode);
                    integratedData.add(newInode);
                } else {
                    logger.error("Could not find anything about {} anywhere. This is makes absolutely no sense..", ld);
                }
            }
        }
        , $integratedDataLock);
    }

    private void scheduleZKReconciler(String esNodeName, boolean startThreadIfNeeded) {
        logger.info("scheduleZKReconciler({}, {})", esNodeName, startThreadIfNeeded);
        performWrite(() -> reconcileQueue.add(esNodeName), $reconcileQueueLock);
        if(zkReconciler.getQueue().size() < 1 && startThreadIfNeeded) {
            zkReconciler.submit(this::reconcileZk);
            logger.info("zkReconcilerTask submitted!");
        }
    }

    /**
     * Runs a loop to reconcile newly added ES nodes with their ZK data.
     */
    private void reconcileZk() {
        AtomicBoolean done = new AtomicBoolean(false);
        while(!done.get()) {
            performWrite(() -> {
                if(reconcileQueue.size() == 0) {
                    logger.warn("reconcileZk is starting its loop but the queue is empty :/");
                }
                List<LeaderData> zkDatas = zkLeaderProvider.getLeaderCandidates();
                Set<String> completed = new HashSet<>();

                List<Tuple<String, AddressPort>> knownESNodes = esClient.getDiscoveredEnkis();

                reconcileQueue.forEach(q -> {
                    LeaderData maybeData = zkDatas.stream()
                            .filter(existsInES(knownESNodes))
                            .filter(LeaderData::isAcceptable)
                            .filter(ld -> ld.getNodeIdentifier().equals(q))
                            .findFirst()
                            .orElse(null);

                    if(maybeData != null) {
                        performWrite(() -> {
                            for (ESZKNode n : integratedData) {
                                if(n.getEsNodeName().equals(q)) {
                                    n.setLeaderData(maybeData);
                                    completed.add(q);
                                    logger.info("Reconciled {} with {}; {} unreconciled remaining.", q, maybeData, reconcileQueue.size() - completed.size());
                                    break;
                                }
                            }
                        },
                        $integratedDataLock);
                    }
                });

                reconcileQueue.removeAll(completed);
            }, $reconcileQueueLock);

            Boolean isDone = optimisticRead(() -> reconcileQueue.size() == 0, $reconcileQueueLock);
            done.set(isDone == null ? false : isDone);

            if(!done.get()) {
                try {
                    Thread.sleep(ZK_RECONCILE_DELAY);
                } catch(InterruptedException e) {
                    logger.error("ZkReconciler was interrupted while sleeping!", e);
                }
            }
        }
    }

    /**
     * Acquires a read lock, runs readOp, and if the data hasn't been updated
     * since readOP finished, returns the result of readOp. Otherwise, it runs readOp
     * again until it has operated on data that has settled.
     * @param readOp the RETRYABLE (SHOULD be idempotent) operation that run
     * @param <T> the type that readOp returns
     * @return the result of readOp after it has ran on a settled state.
     */
    private <T> T optimisticRead(Supplier<T> readOp, StampedLock lock) {
        try {
            T ret;
            boolean isValid;
            do {
                long stamp = lock.tryOptimisticRead();
                Thread.sleep(SETTLE_DELAY);
                ret = readOp.get();
                Thread.sleep(SETTLE_DELAY);
                isValid = lock.validate(stamp);
            } while(!isValid);
            return ret;
        } catch(Exception e) {
            logger.error("Caught an exception while performing an optimistic read of the integrated state, returning null. ", e);
        }
        return null;
    }

    /**
     * Acquires a write lock and performs the operation in r
     * @param r the operation to perform
     * @param lock the StampedLock to lock against
     */
    @SneakyThrows
    private void performWrite(Runnable r, StampedLock lock) {
        long stamp = lock.writeLockInterruptibly();
        try {
            r.run();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Returns a tuple containing whether or not a valid leader is current available.
     * The tuple's first value will be true or false, if it's true, that means there's
     * a valid leader which can be found in the tuple's second value.
     *
     * If it's false, it means no valid leaders are currently available.
     *
     * @return a Tuple containing whether or not there's a valid leader and its data
     */
    private Tuple<Boolean, ESZKNode> validLeaderIfAvailable() {
        Tuple<Boolean, ESZKNode> def = new Tuple<>(false, null);
        Tuple<Boolean, ESZKNode> ret = optimisticRead(() ->
                integratedData.stream()
                              .filter(n -> n.hasLeaderData() && n.isLeader())
                              .findFirst()
                              .map(n -> new Tuple<>(true, n))
                              .orElse(def)
        , $integratedDataLock);

        return ret == null ? def : ret;
    }

    /**
     * Combines ElasticSearch and ZooKeeper metadata and tracks
     * eligibility of one node.
     */
    @EqualsAndHashCode @ToString
    private static final class ESZKNode implements Serializable {
        private static final long serialVersionUID = 7858651108893230473L;
        /**
         * The elasticsearch node of the leader
         */
        private final @Getter String esNodeName;

        /**
         * the candidate's leaderdata
         */
        private volatile @Getter @Setter LeaderData leaderData;

        /**
         * whether or not this node is the leader.
         */
        private volatile @Getter @Setter boolean leader;

        /**
         * Whether or not this node has LeaderData associated with it.
         * @return getLeaderData() != null
         */
        public boolean hasLeaderData() {
            return leaderData != null;
        }

        ESZKNode(String esNodeName, LeaderData leaderData) {
            this(esNodeName, leaderData, false);
        }

        ESZKNode(String esNodeName, LeaderData leaderData, boolean leader) {
            this.esNodeName = esNodeName;
            this.leaderData = leaderData;
            this.leader     = leader;
        }
    }
}
