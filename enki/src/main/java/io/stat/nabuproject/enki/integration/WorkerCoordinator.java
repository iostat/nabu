package io.stat.nabuproject.enki.integration;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.Enki;
import io.stat.nabuproject.enki.integration.balance.AssignmentBalancer;
import io.stat.nabuproject.enki.integration.balance.AssignmentContext;
import io.stat.nabuproject.enki.integration.balance.AssignmentDelta;
import io.stat.nabuproject.enki.server.EnkiServer;
import io.stat.nabuproject.enki.server.NabuConnection;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionEventSource;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionListener;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Listens to ElasticSearch for nodes joining and leaving, assigns work
 * as needed, etc.
 *
 * Like with the leader elector, there are ridiculous amounts of failsafes that
 * are probably beyond redundant and can be optimized out.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class WorkerCoordinator extends Component {
    /**
     * How frequently should the rebalancer run. (currently every 30s)
     * todo: definitely make this tunable?
     */
    public static final int REBALANCE_PERIOD = 10000;

    /**
     * How long to wait for the the rebalance thread to gracefully exist.
     * todo: definitely make this tunable?
     */
    public static final int SHUTDOWN_REBALANCE_KILL_TIMEOUT = 2000;


    private final ThrottlePolicyProvider config;
    private final ESKafkaValidator validator;
    private final ESClient esClient;
    private final EnkiServer enkiServer;
    private final NabuConnectionEventSource nabuConnectionEventSource;
    private final CnxnListener cnxnListener;

    // Get the injector this was created with so we can nuke enki when we halt and catch fire.
    private final Injector injector;

    private final byte[] $assignmentLock;
    private final byte[] $workerLock;

    private final Set<AssignableNabu> confirmedWorkers;
    private final AtomicBoolean needsRebalance;

    private final Set<TopicPartition> $unclaimedAssignments;

    private final Random random;

    private final Thread rebalanceThread;
    private final AtomicBoolean isShuttingDown;

    @Inject
    public WorkerCoordinator(ThrottlePolicyProvider config,
                             ESKafkaValidator validator,
                             ESClient esClient,
                             EnkiServer enkiServer,
                             NabuConnectionEventSource nabuConnectionEventSource,
                             Injector injector) {
        this.config = config;
        this.validator = validator;
        this.esClient = esClient;
        this.enkiServer = enkiServer;
        this.nabuConnectionEventSource = nabuConnectionEventSource;
        this.injector = injector;

        this.cnxnListener = new CnxnListener();

        this.$assignmentLock = new byte[0];
        this.$workerLock     = new byte[0];

        this.$unclaimedAssignments = Sets.newConcurrentHashSet();
        this.confirmedWorkers = Sets.newConcurrentHashSet();
        this.needsRebalance = new AtomicBoolean(false);

        this.random = new Random(System.nanoTime());
        this.isShuttingDown = new AtomicBoolean(false);
        this.rebalanceThread = new Thread(() -> {
            while(!isShuttingDown.get()) {
                try {
                    Thread.sleep(REBALANCE_PERIOD);

                    try {
                        if(needsRebalance.get()) {
                            synchronized ($assignmentLock) {
                                rebalanceAssignments();
                                needsRebalance.set(false);
                            }
                        }
                    } catch (Exception e) {
                        //noinspection ConstantConditions sometimes static analysis is NOT the answer
                        if(!this.isShuttingDown.get() || !(e instanceof InterruptedException)) {
                            logger.error("Received an unexpected exception while performing a rebalance!", e);
                        }
                    }
                } catch(InterruptedException e) {
                    if(isShuttingDown.get()) {
                        logger.info("Received an InterruptedException, but it looks like I'm shutting down.");
                        return;
                    }
                }
            }
        });
        rebalanceThread.setName("WorkerCoordinator-Rebalance");
    }


    @Override
    public void start() throws ComponentException {
        logger.info("WorkerCoordinator start");
        nabuConnectionEventSource.addNabuConnectionListener(cnxnListener);

        synchronized ($assignmentLock) {
            validator.getShardCountCache().values().forEach(esksp -> {
                String topic   = esksp.getTopicName();
                int partitions = esksp.getPartitions();

                for(int i = 0; i < partitions; i++) {
                    $unclaimedAssignments.add(new TopicPartition(topic, i));
                }
            });
        }

        rebalanceThread.start();
        logger.info("WorkerCoordinator start complete");
    }

    @Override
    public void shutdown() throws ComponentException {
        logger.info("WorkerCoordinator shutdown");
        // todo: send unassigns to the Nabus as this node shuts down.
        this.isShuttingDown.set(true);
        try {
            this.rebalanceThread.join(SHUTDOWN_REBALANCE_KILL_TIMEOUT);
            this.rebalanceThread.interrupt();
        } catch(InterruptedException e) {
           logger.error("InterruptedException in shutdown!");
        } finally {
            if(this.rebalanceThread.isAlive()) {
                this.rebalanceThread.interrupt();
                this.rebalanceThread.stop();
            }
            nabuConnectionEventSource.addNabuConnectionListener(cnxnListener);
            logger.info("WorkerCoordinator shutdown complete");
        }
    }

    private void haltAndCatchFire() {
        Thread t = new Thread(() -> { this.shutdown(); injector.getInstance(Enki.class).shutdown(); });
        t.setName("WorkerCoordinator-HCF");
        t.start();
    }

    private void rebalanceAssignments() {
        synchronized ($assignmentLock) {
            try {
                synchronized ($assignmentLock) {
                    if(confirmedWorkers.isEmpty()) {
                        logger.warn("Need to balance {} tasks across 0 workers, what do you actually want from me?", $unclaimedAssignments.size());
                        return;
                    }

                    AssignmentBalancer<AssignableNabu, TopicPartition> balancer = new AssignmentBalancer<>(random);

                    synchronized ($workerLock) {
                        for(AssignableNabu worker : confirmedWorkers) {
                            balancer.addWorker(worker, worker.getAssignments());
                        }
                    }

                    List<AssignmentDelta<AssignableNabu, TopicPartition>> deltas = balancer.balance($unclaimedAssignments);

                    // this is where assignmentdeltas actually get executed
                    for (AssignmentDelta<AssignableNabu, TopicPartition> delta : deltas) {
                        int totalStopsNeeded = delta.getStopCount();
                        CountDownLatch stopCDL = new CountDownLatch(totalStopsNeeded);
                        AtomicBoolean allStopsSucceeded = new AtomicBoolean(true);

                        AssignableNabu n = delta.getContext();

                        for(TopicPartition tp : delta.getToStop()) {
                            // Kick off a stop. A successful stop will trip by the countdown latch by one.
                            // If there's a failure anywhere, allStopsSucceeded will be set to false.
                            // At that point, all of the tasks pending starts will be moved to the
                            // unclaimed queue, to be rebalanced on the next cycle.
                            n.getCnxn().sendUnassign(tp).whenCompleteAsync((response, throwable) -> {
                                logger.debug("{}=>STOP {} :: {}/{}", n.getDescription(), tp, response, throwable);
                                if(throwable != null || response.getType() != EnkiPacket.Type.ACK) {
                                    allStopsSucceeded.set(false);
                                    // trip the countdown latch
                                } else {
                                    n.getAssignments().remove(tp);
                                }

                                // finally trip the stop countdown latch by one
                                stopCDL.countDown();
                            });
                        }

                        // wait for all the stop ops to succeed or fail.
                        stopCDL.await();

                        if(!allStopsSucceeded.get()) {
                            n.getCnxn().killConnection();
                            $unclaimedAssignments.addAll(delta.getToStart());
                            // all of the pre-existing assignment will get reaped
                            // into unclaimed assignments when onNabuDisconnected callback
                            // is called after the connection is dropped.
                        } else {
                            int totalStartsNeeded = delta.getStartCount();
                            CountDownLatch startCDL = new CountDownLatch(totalStartsNeeded);
                            AtomicBoolean allStartsSucceeded = new AtomicBoolean(true);
                            Set<TopicPartition> pendingStarts = Sets.newHashSet(delta.getToStart());

                            for(TopicPartition tp : pendingStarts) {
                                n.getCnxn().sendAssign(tp).whenCompleteAsync((response, throwable) -> {
                                    logger.debug("{}=>START {} :: {}/{}", n.getDescription(), tp, response, throwable);
                                    if(throwable != null || response.getType() != EnkiPacket.Type.ACK) {
                                        allStartsSucceeded.set(false);
                                    } else {
                                        pendingStarts.remove(tp);
                                        n.getAssignments().add(tp);
                                    }

                                    // finally, trip the countdown latch by one
                                    startCDL.countDown();
                                });
                            }

                            startCDL.await();
                            if(!allStartsSucceeded.get()) {
                                n.getCnxn().killConnection();
                                $unclaimedAssignments.addAll(pendingStarts);
                            }
                        }
                    }

                    logger.info("Rebalance cycle finished.");
                }
            } catch(AssertionError | Exception e) {
                logger.error("[FATAL] something went wrong when rebalancing the workload", e);
                logger.error("[FATAL] halting and catching fire.");
                haltAndCatchFire();
            }
        }
    }

    public void coordinateJoin(NabuConnection cnxn) {
        synchronized($workerLock) {
            AssignableNabu n = new AssignableNabu(cnxn);
            confirmedWorkers.add(n);
            needsRebalance.set(true);
        }
    }

    public void coordinatePart(NabuConnection cnxn) {
        synchronized($workerLock) {
            boolean shouldRebalanceAfter = false;
            Iterator<AssignableNabu> workers = confirmedWorkers.iterator();
            while (workers.hasNext()) {
                AssignableNabu worker = workers.next();
                if(worker.getCnxn() == cnxn) { // yes, testing reference equality.
                    synchronized ($assignmentLock) {
                        Set<TopicPartition> assns = worker.getAssignments();
                        $unclaimedAssignments.addAll(assns);
                    }
                    workers.remove();
                    shouldRebalanceAfter = true;
                }
            }
            if(shouldRebalanceAfter) {
                needsRebalance.set(true);
            }
        }
    }

    private static class AssignableNabu implements AssignmentContext<TopicPartition> {
        private final @Getter NabuConnection cnxn;
        private @Getter @Setter Set<TopicPartition> assignments;

        AssignableNabu(NabuConnection cnxn) {
            this.cnxn = cnxn;
            this.assignments = Sets.newHashSet();
        }

        @Override
        public String getDescription() {
            return cnxn.prettyName();
        }

        @Override
        public String collateAssignmentsReadably(Set<TopicPartition> allAssignments) {
            Map<String, List<Integer>> collateMap = Maps.newHashMap();
            allAssignments.forEach(assn -> {
                String topic = assn.topic();
                int part = assn.partition();
                if (collateMap.containsKey(topic)) {
                    collateMap.get(topic).add(part);
                } else {
                    collateMap.put(topic, Lists.newArrayList(part));
                }
            });
            List<String> collatedStrings = Lists.newArrayList();
            collateMap.forEach((k, v) -> {
                v.sort(Comparator.naturalOrder());
                String joined = k + "[" + Joiner.on(',').join(v) + "]";
                collatedStrings.add(joined);
            });

            return Joiner.on(", ").join(collatedStrings);
        }
    }

    private class CnxnListener implements NabuConnectionListener {
        @Override
        public boolean onNewNabuConnection(NabuConnection cnxn) {
            return true;
        }

        @Override
        public boolean onNabuLeaving(NabuConnection cnxn, boolean serverInitiated) { return true; }

        @Override
        public boolean onNabuDisconnected(NabuConnection cnxn, EnkiConnection.DisconnectCause cause, boolean wasAcked) {
            coordinatePart(cnxn);
            return true;
        }

        @Override
        public boolean onPacketDispatched(NabuConnection cnxn, EnkiPacket packet, CompletableFuture<EnkiPacket> future) { return true; }

        @Override
        public boolean onPacketReceived(NabuConnection cnxn, EnkiPacket packet) { return true; }

        @Override
        public boolean onNabuReady(NabuConnection cnxn) {
            WorkerCoordinator.this.coordinateJoin(cnxn);
            return true;
        }
    }
}
