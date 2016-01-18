package io.stat.nabuproject.enki.integration;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
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
import io.stat.nabuproject.core.telemetry.TelemetryCounterSink;
import io.stat.nabuproject.core.telemetry.TelemetryGaugeSink;
import io.stat.nabuproject.core.telemetry.TelemetryService;
import io.stat.nabuproject.core.throttling.ThrottlePolicyChangeListener;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.Enki;
import io.stat.nabuproject.enki.integration.balance.AssignmentBalancer;
import io.stat.nabuproject.enki.integration.balance.AssignmentContext;
import io.stat.nabuproject.enki.integration.balance.AssignmentDelta;
import io.stat.nabuproject.enki.leader.ElectedLeaderProvider;
import io.stat.nabuproject.enki.server.EnkiServer;
import io.stat.nabuproject.enki.server.NabuConnection;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionEventSource;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionListener;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
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
import java.util.concurrent.TimeUnit;
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
public class WorkerCoordinator extends Component implements ThrottlePolicyChangeListener {
    /**
     * How frequently should the rebalancer run. (currently every 30s)
     * todo: definitely make this tunable?
     */
    public static final int REBALANCE_PERIOD = 5000;

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

    private final ElectedLeaderProvider elp;
    // Get the injector this was created with so we can nuke enki when we halt and catch fire.
    private final Injector injector;

    private final byte[] $lock;

    private final Set<AssignableNabu> confirmedWorkers;
    private final AtomicBoolean needsRebalance;

    private Set<TopicPartition> $allAssignments;

    private final Random random;

    private final Thread rebalanceThread;
    private final AtomicBoolean isShuttingDown;

    private final TelemetryGaugeSink connectedNabuGauge;
    private final TelemetryCounterSink reapedNabuCount;
    private final TelemetryCounterSink rebalanceCounter;

    @Inject
    public WorkerCoordinator(ThrottlePolicyProvider config,
                             TelemetryService ts,
                             ESKafkaValidator validator,
                             ESClient esClient,
                             EnkiServer enkiServer,
                             NabuConnectionEventSource nabuConnectionEventSource,
                             ElectedLeaderProvider elp,
                             Injector injector) {
        this.config = config;
        this.validator = validator;
        this.esClient = esClient;
        this.enkiServer = enkiServer;
        this.nabuConnectionEventSource = nabuConnectionEventSource;
        this.elp = elp;
        this.injector = injector;

        this.cnxnListener = new CnxnListener();

        this.connectedNabuGauge = ts.createGauge("connected");
        this.reapedNabuCount    = ts.createCounter("reaped");
        this.rebalanceCounter   = ts.createCounter("rebalances");

        this.$lock = new byte[0];

        this.$allAssignments = null;
        this.confirmedWorkers = Sets.newHashSet();
        this.needsRebalance = new AtomicBoolean(false);

        this.random = new Random(System.nanoTime());
        this.isShuttingDown = new AtomicBoolean(false);
        this.rebalanceThread = new Thread(() -> {
            while(!isShuttingDown.get()) {
                try {
                    Thread.sleep(REBALANCE_PERIOD);

                    try {
                        while(needsRebalance.get()) {
                            synchronized ($lock) {
                                rebalanceAssignments();
                            }
                            if(needsRebalance.get()) {
                                // backoff for re-rebalance
                                // 1 second to grab the worker set lock to coordinate a join.
                                Thread.sleep(1000);
                            }
                            rebalanceCounter.increment();
                        }
                    } catch (Exception e) {
                        //noinspection ConstantConditions sometimes static analysis is NOT the answer
                        if(!(this.isShuttingDown.get() && (e instanceof InterruptedException))) {
                            logger.error("Received an unexpected exception while performing a rebalance!", e);
                            throw e;
                        }
                    }
                } catch(Exception e) {
                    if(isShuttingDown.get() && e instanceof InterruptedException) {
                        logger.info("Received an InterruptedException, but it looks like I'm shutting down.");
                        return;
                    } else {
                        logger.error("Unexpected Exception!", e);
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
        config.registerThrottlePolicyChangeListener(this);

        synchronized ($lock) {
            ImmutableSet.Builder<TopicPartition> allAssnBuilder = ImmutableSet.builder();
            validator.getShardCountCache().values().forEach(esksp -> {
                String topic   = esksp.getTopicName();
                int partitions = esksp.getPartitions();

                for(int i = 0; i < partitions; i++) {
                    allAssnBuilder.add(new TopicPartition(topic, i));
                }
            });

            $allAssignments = allAssnBuilder.build();
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
            nabuConnectionEventSource.removeNabuConnectionListener(cnxnListener);
            config.deregisterThrottlePolicyChangeListener(this);
            logger.info("WorkerCoordinator shutdown complete");
        }
    }

    private void haltAndCatchFire() {
        Thread t = new Thread(() -> { this.shutdown(); injector.getInstance(Enki.class).shutdown(); });
        t.setName("WorkerCoordinator-HCF");
        t.start();
    }

    private void rebalanceAssignments() {
        synchronized ($lock) {
            try {
                Set<TopicPartition> unclaimedAssignments = Sets.newHashSet($allAssignments);
                boolean hadReaps = false;

                if (confirmedWorkers.isEmpty()) {
                    logger.warn("Need to balance {} tasks across 0 workers, what do you actually want from me?", unclaimedAssignments.size());
                    return;
                }

                AssignmentBalancer<AssignableNabu, TopicPartition> balancer = new AssignmentBalancer<>(random);
                for (AssignableNabu worker : confirmedWorkers) {
                    worker.getAssignments().forEach(unclaimedAssignments::remove);
                    balancer.addWorker(worker, worker.getAssignments());
                }

                @RequiredArgsConstructor @EqualsAndHashCode
                class ContextAssignment {
                    private final AssignableNabu context;
                    private final TopicPartition assignment;
                }

                List<AssignmentDelta<AssignableNabu, TopicPartition>> deltas = balancer.balance(unclaimedAssignments);
                List<ContextAssignment> allStops = Lists.newArrayList();
                List<ContextAssignment> allStarts = Lists.newArrayList();

                deltas.forEach(delta -> {
                    delta.getToStart().forEach(start ->
                        allStarts.add(new ContextAssignment(delta.getContext(), start))
                    );

                    delta.getToStop().forEach(stop ->
                        allStops.add(new ContextAssignment(delta.getContext(), stop))
                    );
                });

                for(ContextAssignment ca : allStops) {
                    AssignableNabu n = ca.context;
                    TopicPartition tp = ca.assignment;
                    CountDownLatch latch = new CountDownLatch(1);
                    AtomicBoolean stopSucceeded = new AtomicBoolean(true);
                    n.getCnxn().sendUnassign(tp).whenCompleteAsync((response, throwable) -> {
                        synchronized (n.$workerAssignmentLock) {
                            logger.debug("{}=>STOP {} :: {}/{}", n.getDescription(), tp, response, throwable);
                            if (throwable != null || response.getType() != EnkiPacket.Type.ACK) {
                                stopSucceeded.set(false);
                            }

                            // always remove the assignment, as even if the worker gets reaped
                            // it's going to not have the assignment anyway by the time
                            // the next rebalance starts
                            n.getAssignments().remove(tp);
                            // finally trip the stop countdown latch by one
                        }
                        latch.countDown();
                    });

                    boolean stopLatchTimedOut = false;
                    try {
                        stopLatchTimedOut = !latch.await(5, TimeUnit.SECONDS);
                    } catch(InterruptedException e) {
                        logger.warn("Countdown latch interrupted waiting for a stop operation. Killing connection");
                        stopSucceeded.set(false);
                    }

                    if(stopLatchTimedOut) {
                        logger.warn("Countdown latch timed out waiting for a stop operation. Killing connection");
                        stopSucceeded.set(false);
                    }

                    if(!stopSucceeded.get()) {
                        reapMisbehavingWorker(n);
                        hadReaps = true;
                    }
                }

                for(ContextAssignment ca : allStarts) {
                    AssignableNabu n = ca.context;
                    TopicPartition tp = ca.assignment;
                    CountDownLatch latch = new CountDownLatch(1);
                    AtomicBoolean startSucceeded = new AtomicBoolean(true);
                    n.getCnxn().sendAssign(tp).whenCompleteAsync((response, throwable) -> {
                        synchronized (n.$workerAssignmentLock) {
                            logger.debug("{}=>STOP {} :: {}/{}", n.getDescription(), tp, response, throwable);
                            if (throwable != null || response.getType() != EnkiPacket.Type.ACK) {
                                startSucceeded.set(false);
                            }

                            // always add the assignment, as even if the worker gets reaped
                            // it's going to not have the assignment anyway by the time
                            // the next rebalance starts
                            n.getAssignments().add(tp);
                            // finally trip the stop countdown latch by one
                            latch.countDown();
                        }
                    });

                    boolean startLatchTimedOut = false;
                    try {
                        startLatchTimedOut = !latch.await(5, TimeUnit.SECONDS);
                    } catch(InterruptedException e) {
                        logger.warn("Countdown latch interrupted waiting for a start operation. Killing connection");
                        startSucceeded.set(false);
                    }

                    if(startLatchTimedOut) {
                        logger.warn("Countdown latch timed out waiting for a start operation. Killing connection");
                        startSucceeded.set(false);
                    }

                    if(!startSucceeded.get()) {
                        reapMisbehavingWorker(n);
                        hadReaps = true;
                    }
                }

                needsRebalance.set(hadReaps);

                StringBuilder sb = new StringBuilder("\nEXECUTED TASK DISTRIBUTION\n");
                sb.append(Strings.padStart("WORKER NAME", 60, ' '))
                  .append(Strings.padStart("FINAL TASK COUNT", 20, ' '))
                  .append("      TASKS")
                  .append("\n");

                confirmedWorkers.forEach(worker ->
                            sb.append(Strings.padStart(worker.getDescription(), 60, ' '))
                              .append(Strings.padStart(Integer.toString(worker.getAssignments().size()), 20, ' '))
                              .append("      ")
                              .append(worker.collateAssignmentsReadably(worker.getAssignments(), ImmutableSet.of(), ImmutableSet.of()))
                              .append("\n")
                );

                logger.info(sb.toString());

                if(!hadReaps) {
                    logger.info("Rebalance cycle finished.");
                } else {
                    logger.info("Misbehaving workers were reaped during this cycle. Restarting it.");
                }
            } catch (AssertionError | Exception e) {
                logger.error("[FATAL] something went wrong when rebalancing the workload", e);
                logger.error("[FATAL] halting and catching fire.");
                haltAndCatchFire();
            }
        }
    }

    private void reapMisbehavingWorker(AssignableNabu n) {
        synchronized ($lock) {
            n.getCnxn().killConnection();
            confirmedWorkers.removeIf(p -> {
                if(n.getCnxn().prettyName().equals(p.getCnxn().prettyName())) {
                    logger.warn("REAPING MISBEHAVING WORKER: {}", n);
                    return true;
                }
                return false;
            });

            reapedNabuCount.increment();
        }
    }

    public void coordinateJoin(NabuConnection cnxn) {
        synchronized($lock) {
            if(elp.isSelf()) {
                AssignableNabu n = new AssignableNabu(cnxn);
                confirmedWorkers.add(n);
                needsRebalance.set(true);
                connectedNabuGauge.set(confirmedWorkers.size());
            }
        }
    }

    public void coordinatePart(NabuConnection cnxn) {
        synchronized($lock) {
            Iterator<AssignableNabu> workers = confirmedWorkers.iterator();
            while (workers.hasNext()) {
                AssignableNabu worker = workers.next();
                if(worker.getCnxn().prettyName().equals(cnxn.prettyName())) {
                    workers.remove();
                    needsRebalance.set(true);

                    return;
                }
            }

            connectedNabuGauge.set(confirmedWorkers.size());
        }
    }

    @Override
    public boolean onThrottlePolicyChange() {
        logger.info("Notified of throttle policy change!");
        synchronized ($lock) {
            confirmedWorkers.forEach(anabu -> {
                NabuConnection cnxn = anabu.getCnxn();
                cnxn.refreshConfiguration();
            });
        }
        return true;
    }

    @EqualsAndHashCode(exclude={"assignments"})
    private static class AssignableNabu implements AssignmentContext<TopicPartition> {
        private final @Getter NabuConnection cnxn;
        private volatile @Getter @Setter Set<TopicPartition> assignments;
        private final byte[] $workerAssignmentLock;

        AssignableNabu(NabuConnection cnxn) {
            this.cnxn = cnxn;
            this.assignments = Sets.newHashSet();
            this.$workerAssignmentLock = new byte[0];
        }

        @Override
        public String getDescription() {
            return cnxn.prettyName();
        }

        private static void addToCollateMap(Map<String, List<String>> collateMap, Set<TopicPartition> items, String prefix) {
            items.forEach(assn -> {
                String topic = assn.topic();
                int part = assn.partition();
                if (collateMap.containsKey(topic)) {
                    collateMap.get(topic).add(prefix + Integer.toString(part));
                } else {
                    collateMap.put(topic, Lists.newArrayList(prefix + Integer.toString(part)));
                }
            });
        }
        @Override
        public String collateAssignmentsReadably(Set<TopicPartition> startedWith, Set<TopicPartition> toStart, Set<TopicPartition> toStop) {
            Map<String, List<String>> collateMap = Maps.newHashMap();
            addToCollateMap(collateMap, startedWith, "");
            addToCollateMap(collateMap, toStart, "+");
            addToCollateMap(collateMap, toStop, "-");

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
            logger.info("DC! {} {}", cnxn.prettyName(), cause);
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
