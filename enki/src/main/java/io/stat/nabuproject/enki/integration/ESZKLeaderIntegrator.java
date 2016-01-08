package io.stat.nabuproject.enki.integration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEvent;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEventListener;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import io.stat.nabuproject.core.util.Tuple;
import io.stat.nabuproject.core.util.concurrent.ResettableCountDownLatch;
import io.stat.nabuproject.enki.Enki;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.stat.nabuproject.core.util.functional.FluentCompositions.$;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.$_$;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.curry2c;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.fmap;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.masquerade;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.on;
import static io.stat.nabuproject.core.util.functional.FunMath.eq;
import static io.stat.nabuproject.core.util.functional.FunMath.lcmp;

/**
 * Integrates ElasticSearch cluster state and ZooKeeper leader election events
 * to provide a more reliable/current leader state. The implementations for
 * {@link ElectedLeaderProvider} will all block until reliable data is known.
 *
 * todo: there are probably a TON of redundant operations which can be optimized to hell and back
 * but I don't really care for it at the moment. better safe than sorry right?
 *
 * Also, there are a lot of delays that may be unecessary, and possibly even contribute to 5s+ delays for leader reelection
 * I don't really want to focus on fixing it at the moment, as that short moment with no elected leader isn't really THAT frightening.
 * Might even be a good thing, allowing Nabu's to settle down a bit before reconnecting.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class ESZKLeaderIntegrator extends LeaderLivenessIntegrator implements LeaderEventListener, NabuESEventListener {
    /**
     * Helper function to shorten LeaderData into a more readable toString()
     */
    private static final Function<LeaderData, String> SHORTEN_LEADER_DATA = ld ->
            ld == null ? "null" : String.format("%d[%s/%s:%d]",
                    ld.getPriority(),
                    ld.getNodeIdentifier(),
                    ld.getAddressPort().getAddress(),
                    ld.getAddressPort().getPort());
    /**
     * SHORTEN_LEADER_DATA composed to pull LeaderData from an ESZKNode
     */
    private static final Function<ESZKNode, String> SHORTEN_ESZKNODE = $_$(ESZKNode::getLeaderData, SHORTEN_LEADER_DATA);

    /**
     * Shorthand for unflagging an ESZKNode as the leader inside functional APIs
     */
    private static final Consumer<ESZKNode> UNSET_LEADER_FLAG = curry2c(ESZKNode::setLeader, false);

    /**
     * Shorthand for flagging an ESZKNode as the leader inside functional APIs
     */
    private static final Consumer<ESZKNode> SET_LEADER_FLAG = curry2c(ESZKNode::setLeader, true);

    // todo: should these be tunable?
    /**
     * Amount of time to sleep before and after each read to allow write operations to happen
     * before data is returned.
     * The idea being, this reduces the risk of a race condition leading to stale data wherein nothing has updated
     * the integrated leader state prior to a read lock being obtained, or it was updated shortly after the read
     * finished.
     */
    public static final int SETTLE_DELAY = 1000;

    /**
     * How long to wait before retrying ZK reconciliation queue
     * entries if there's no match yet.
     */
    public static final int ZK_RECONCILE_DELAY = 500;

    /**
     * How to long to wait before retrying finding
     * a leader while one is not available.
     */
    public static final int NO_LEADER_RETRY_DELAY = 2000;

    /**
     * Maximum amount of times to retry getting a leader before giving up
     */
    public static final int MAX_LEADER_RETRIES = 10;

    /**
     * How long to hold any leader requests after a node departs from ES to allow ZK to try and catch up.
     */
    public static final int ES_EVENT_SYNC_DELAY = 750;

    /**
     * How long between when ES_EVENT_SYNC_DELAYs can be re-triggered. Used in order
     * to prevent a flapping Enki from DoSing provisioning of leaders to Nabus
     */
    public static final int ES_EVENT_SYNC_DELAY_MINIMUM_GAP = 30;

    /**
     * How long to allow a cache-buster task to run when ZK says we've been demoted
     * before considering it too big a risk of split-brain and killing the app.
     */
    public static final int MAX_DEMOTION_FAILSAFE_TIMEOUT = 12000;

    private final ESClient esClient;
    private final ZKLeaderProvider zkLeaderProvider;
    private final Injector injector;
    private final StampedLock $integratedDataLock;
    private volatile Set<ESZKNode> integratedData;

    private final ThreadPoolExecutor zkReconciler;
    private final StampedLock $reconcileQueueLock;
    private final Set<String> reconcileQueue;

    private final ResettableCountDownLatch esEventSyncDelayLatch;
    private final AtomicLong lastESEventSyncDelayCompleted;

    private static Predicate<LeaderData> existsInES(List<Tuple<String, AddressPort>> knownFromES) {      /* **************************** */
        // for the candidate that we are testing                                             gave u      *          oh noes!            *
        return candidate -> knownFromES.stream() // see if                        the ES client    s     * theres a leak in the stream  *
        /*              |  \        */.anyMatch(t -> // any of the    of the tuples     ___________  h   * and the bytes are all wonky! *
        /*              |   \       **********/t.first() // first elements             // ____h c t\ a   * **************************** *
        /*              |\   \  ~~ thanks   */.equals(candidate.getNodeIdentifier()));// /  i     \a\v   /
        //    *****    *\*|   \     chesapeake energy co! ~~                         // /    n g   \me  /
        // **************\|    \____________________________________________________// /   n a m e s\  /
        // ***************\___________________________________________________________________________/
    }

    @Inject
    ESZKLeaderIntegrator(ESClient esClient, ZKLeaderProvider zkLeaderProvider, Injector injector) {
        this.esClient = esClient;
        this.zkLeaderProvider = zkLeaderProvider;
        this.injector = injector;
        this.$integratedDataLock = new StampedLock();

        this.integratedData = Collections.synchronizedSet(Sets.newConcurrentHashSet());

        this.zkReconciler = new ThreadPoolExecutor(
                1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(20),
                new NamedThreadFactory("ZKLeaderReconciler")
        );
        this.$reconcileQueueLock = new StampedLock();
        this.reconcileQueue = Sets.newConcurrentHashSet();

        // Reset the latch immediately, to prevent a deadlock.
        this.esEventSyncDelayLatch = new ResettableCountDownLatch(1);
        this.esEventSyncDelayLatch.countDown();
        this.lastESEventSyncDelayCompleted = new AtomicLong(System.currentTimeMillis());

        logger.info("ESZKLeaderIntegrator constructed!");
    }

    @Override
    public void start() throws ComponentException {
        performWrite(() -> {
            integratedData.clear();

            Iterator<Tuple<String, AddressPort>> tupIt = esClient.getDiscoveredEnkis().iterator();
            // don't start the reconciler until the list of currently known ES nodes has been seeded.
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
//        logger.info("ESZKLeaderIntegrator#isSelf()");
        return validLeaderIfAvailable().map(l -> l.getLeaderData().equals(getOwnLeaderData())).orElse(false);
    }

    @Override
    public List<LeaderData> getLeaderCandidates() {
        waitForEsSyncUnlatch();
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
        waitForEsSyncUnlatch();
        String myESNodeName = esClient.getESIdentifier();
        return optimisticRead(() ->
                integratedData.stream()
                              .filter(n -> n.hasLeaderData() && n.getEsNodeName().equals(myESNodeName))
                              .findFirst()
                              .map(ESZKNode::getLeaderData)
                              .orElse(zkLeaderProvider.getElectedLeaderData())
                , $integratedDataLock);
    }

    @Override @SneakyThrows
    public LeaderData getElectedLeaderData() {
//        logger.info("ESZKLeaderIntegrator#getElectedLeaderData()");
        LeaderData ret = getOwnLeaderData();
        int tries = 0;
        while(tries < MAX_LEADER_RETRIES) {
            Optional<ESZKNode> maybe = validLeaderIfAvailable();
            if(maybe.isPresent()) {
                return maybe.get().getLeaderData();
            }
            Thread.sleep(NO_LEADER_RETRY_DELAY);
            tries++;
        }

        List<String> copy = optimisticRead(() -> ImmutableList.copyOf(fmap(SHORTEN_ESZKNODE, integratedData).iterator()), $integratedDataLock);
        logger.error("Couldn't find a valid leader after {} tries... Sending self as leader, but this may be VERY wrong\n" +
                "My most current data is: {}", MAX_LEADER_RETRIES, copy);

        return ret;
    }

    /**
     * Finds the node with the lowest "priority"
     * which coincidentally is the ZNode number in the only
     * implementation we have
     * @param allLeaders a list of all potential leaders ZK is aware of
     * @return {@code Optional<LeaderData>}
     */
    private Optional<LeaderData> findTrueLeader(List<LeaderData> allLeaders) {
//        logger.info("ESZKLeaderIntegrator#findTrueLeader()");

        // This seek is slightly different from ZKImpls. This finds the lowest priority
        // (i.e., ZK ZNode sequence) node that's been reconciled with ES,
        //
        // sleep a bit to allow ES events to reconcile
        if(reconcileQueue.size() > 0) {
            try { Thread.sleep(ES_EVENT_SYNC_DELAY + ZK_RECONCILE_DELAY); } catch (Exception ignored) { }
        }
        return optimisticRead(() -> {
            List<Tuple<String, AddressPort>> knownEsNodes = esClient.getDiscoveredEnkis();
            return allLeaders.stream()
                    .filter((existsInES(knownEsNodes))
                       .and(al -> (integratedData.stream()
                                                 .anyMatch(i ->
                                                         i.getEsNodeName()
                                                        .equals(al.getNodeIdentifier())))))
                    .sorted(on(LeaderData::getPriority, Longs::compare))
                    .findFirst();
        }, $integratedDataLock);
    }

    /**
     * Called by ZK if there's any activity in the leader election ZNode
     * @param isSelf whether or not this node is allegedly the new leader.
     * @param allegedLeader the LeaderData of the new alleged leader
     * @param allLeaderData the leaderDatas of all the nodes.
     * @return always true
     */
    @Override
    public boolean onLeaderChange(boolean isSelf, LeaderData allegedLeader, List<LeaderData> allLeaderData) {
        LeaderData myData = getOwnLeaderData();
        LeaderData trueLeader = findTrueLeader(zkLeaderProvider.getLeaderCandidates()).orElse(null);

        boolean trueLeaderEqZkLead = (trueLeader != null && allegedLeader != null && trueLeader.equals(allegedLeader));

        String allegedLeaderShort = SHORTEN_LEADER_DATA.apply(allegedLeader);
        String trueLeaderShort = SHORTEN_LEADER_DATA.apply(trueLeader);
        String myLeaderDataShort = SHORTEN_LEADER_DATA.apply(myData);

        synchronized (logger) {
            logger.info("ZK says {}:{} is allegedly the leader, and my check says its: {}", isSelf ? "self" : "other", allegedLeaderShort, trueLeaderEqZkLead);
            logger.info("ESZK:{} {} ZK:{} (self:{})", trueLeaderShort, trueLeaderEqZkLead ? "==" : "!=", allegedLeaderShort, myLeaderDataShort);
        }

        setAsLeader(trueLeader);

        return true;
    }

    @Override
    public boolean onSelfDemoted(LeaderData lastKnownData) {
        /**
         *  as a failsafe, we start a new thread, and clear
         *  all the caches with it. If the thread takes more
         *  than MAX_DEMOTION_FAILSAFE_TIMEOUT, we kill the
         *  the application by returning false and failing
         *  the callback.
         */
        Thread t = new Thread(() -> {
            long stamp = 0;
            try {
                stamp = $integratedDataLock.writeLockInterruptibly();
                integratedData = integratedData.stream()
                                               .filter(eszk ->
                                                   !eszk.getEsNodeName().equals(getOwnLeaderData().getNodeIdentifier())
                                               )
                                               .map(masquerade(UNSET_LEADER_FLAG))
                                               .collect(Collectors.toSet());

            } catch(Exception e) {
                logger.error("Something went terribly wrong while busting caches after a demotion.", e);
                injector.getInstance(Enki.class).shutdown();
            } finally {
                $integratedDataLock.unlock(stamp);
            }
        });
        t.setName("Demotion-Cachebuster");

        try {
            Thread.sleep(MAX_DEMOTION_FAILSAFE_TIMEOUT);
        } catch(InterruptedException e) {
            logger.error("Caught an interrupt while waiting for the demotion failsafe to finish!", e);
        } finally {
            if(t.isAlive()) {
                t.interrupt();
                injector.getInstance(Enki.class).shutdown();
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean onNabuESEvent(NabuESEvent event) {
        String nodeName = event.getNode().getName();
        if(event.getType() == NabuESEvent.Type.ENKI_JOINED) {
            // a node that can be seen joining
            // joined after this node
            // and thus would have a later sequence in ZK
            // thus there is no need to re-validate the leader
            // as it has no chance to become the leader.
            logger.info("New Enki joined! ({})", nodeName);
            registerEsNode(nodeName, true, true);
        } else if (event.getType() == NabuESEvent.Type.ENKI_PARTED) {
            logger.info("Enki \"{}\" departed ES cluster.", nodeName);
            performWrite(() -> {
                performWrite(() -> reconcileQueue.removeIf(nn -> nn.equals(nodeName)), $reconcileQueueLock);
                integratedData.removeIf(node -> {
                    if(node.getEsNodeName().equals(nodeName)) {
                        // need to unset the leader flag before removing from integrated data
                        // because for whatever fucktarded reason java is keeping a thread local (true)
                        // or fucking something. no clue why. but this works. and none of the volatile or
                        // synchronized definitions or fucking anything else works.
                        UNSET_LEADER_FLAG.accept(node);
                    }
                    return false;
                });
            }, $integratedDataLock);

            triggerEsEventSyncDelay();

            String testLD = validLeaderIfAvailable().map(SHORTEN_ESZKNODE).orElse("null");
            Thread threadify = new Thread(() -> { logger.info("{}", ESZKLeaderIntegrator.this.getElectedLeaderData()); });
            synchronized(logger) {
                logger.info("Immediate call to validLeaderAvailable returned: {}", testLD);
                logger.info("If the node that just left was the leader, the result should be null, " +
                            "unless there was a ZK change event fired immediately before this test.");
                logger.info("If the node that left was not the leader or no ZK event happened immediately beforehand, " +
                            "tell a police officer or an MTA employee.");
            }
        }

        return true;
    }

    private void triggerEsEventSyncDelay() {
        synchronized(esEventSyncDelayLatch) {
            if((System.currentTimeMillis() - lastESEventSyncDelayCompleted.get()) < ES_EVENT_SYNC_DELAY_MINIMUM_GAP) {
                // flapping Enkis shouldn't lead to ridiculous DoSs by retriggering this latch.
                return;
            }

            this.esEventSyncDelayLatch.reset();
            Thread t = new Thread(() -> {
                try {
                    Thread.sleep(ES_EVENT_SYNC_DELAY);
                } catch (Exception e) {
                    logger.warn("ES event synchronization delay unlatch() was interrupted!", e);
                } finally {
                    this.esEventSyncDelayLatch.countDown();
                }
            });
            t.setName("ESZKIntegrator-ESEventSyncDelayUnlatcher");
            t.start();
        }
    }

    private void waitForEsSyncUnlatch() {
        try {
            Thread.sleep(ES_EVENT_SYNC_DELAY);
        } catch (Exception e) {
            logger.warn("ES event synchronization delay wait() was interrupted!", e);
        }
    }

    private void registerEsNode(String nodeName, boolean scheduleReconciler, boolean startReconciler) {
//        logger.info("ESZKLeaderIntegrator#registerEsNode({}, {}, {})", nodeName, scheduleReconciler, startReconciler);
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
                        SET_LEADER_FLAG.accept(inode);
                        seen = true;
                        logger.info("Reconciled ESZK link for {}", SHORTEN_ESZKNODE.apply(inode));
                    }
                } else {
                    if (inode.getLeaderData().equals(ld)) {
                        SET_LEADER_FLAG.accept(inode);
                        seen = true;
                    } else {
                        UNSET_LEADER_FLAG.accept(inode);
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
        performWrite(() -> reconcileQueue.add(esNodeName), $reconcileQueueLock);
        if(zkReconciler.getQueue().size() < 1 && startThreadIfNeeded) {
            zkReconciler.submit(this::reconcileZk);
//            logger.info("zkReconcilerTask submitted!");
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
                    return;
                }
                List<LeaderData> zkDatas = zkLeaderProvider.getLeaderCandidates();
                Set<String> completed = new HashSet<>();

                List<Tuple<String, AddressPort>> knownESNodes = esClient.getDiscoveredEnkis();

                reconcileQueue.forEach(q -> {
                    //noinspection unchecked -> eq is a standard Object equality predicate
                    Predicate<String> equalsQueued = (Predicate)eq(q);
                    Optional<LeaderData> maybeData =
                            zkDatas.stream()
                                .filter(     (existsInES(knownESNodes))
                                        .and(LeaderData::isAcceptable)
                                     .and($_$(LeaderData::getNodeIdentifier, equalsQueued)))
                                .findFirst();

                    if(maybeData.isPresent()) {
                        LeaderData data = maybeData.get();
                        performWrite(() -> {
                            for (ESZKNode n : integratedData) {
                                if(equalsQueued.test(n.getEsNodeName())) {
                                    n.setLeaderData(data);
                                    completed.add(q);
                                    logger.info("Reconciled {} with {}; {} unreconciled remaining.",
                                            q,
                                            SHORTEN_LEADER_DATA.apply(data),
                                            reconcileQueue.size() - completed.size());
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
     * Returns an ESZKNode in the Optional monad
     *
     * @return an {@code Optional<ESZKNode>}
     */
    private Optional<ESZKNode> validLeaderIfAvailable() {
        waitForEsSyncUnlatch();
        reassertSingleLeader();
        return optimisticRead(() ->
                integratedData.stream()
                              .filter($    (ESZKNode::hasLeaderData)
                                      .and(
                                           (ESZKNode::isLeader)))
                              .findFirst()
        , $integratedDataLock);
    }

    private void reassertSingleLeader() {
        performWrite(() -> {
            List<ESZKNode> foundNodes = (integratedData.stream()
                                                       .filter(ESZKNode::isLeader)
                                                       .collect(Collectors.toList()));

            if(foundNodes.size() > 1) {
                foundNodes.sort(on($_$(ESZKNode::getLeaderData, LeaderData::getPriority), lcmp));
                foundNodes.stream().skip(1).forEach(UNSET_LEADER_FLAG);
            }
        }, $integratedDataLock);
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
