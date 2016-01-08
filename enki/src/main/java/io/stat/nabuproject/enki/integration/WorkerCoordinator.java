package io.stat.nabuproject.enki.integration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.elasticsearch.ESEventSource;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEvent;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEventListener;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.Enki;
import io.stat.nabuproject.enki.integration.balance.AssignmentBalancer;
import io.stat.nabuproject.enki.integration.balance.AssignmentDelta;
import io.stat.nabuproject.enki.server.EnkiServer;
import io.stat.nabuproject.enki.server.NabuConnection;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionEventSource;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionListener;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
public class WorkerCoordinator extends Component implements NabuESEventListener {
    private final ThrottlePolicyProvider config;
    private final ESKafkaValidator validator;
    private final ESClient esClient;
    private final ESEventSource esEventSource;
    private final EnkiServer enkiServer;
    private final NabuConnectionEventSource nabuConnectionEventSource;
    private final CnxnListener cnxnListener;

    // Get the injector this was created with so we can nuke enki when we halt and catch fire.
    private final Injector injector;

    private final byte[] $assignmentLock;

    private final Set<AssignableNabu> confirmedWorkers;
    private final AtomicBoolean needsRebalance;

    private final Set<TopicPartition> $unclaimedAssignments;

    @Inject
    public WorkerCoordinator(ThrottlePolicyProvider config,
                             ESKafkaValidator validator,
                             ESClient esClient,
                             ESEventSource esEventSource,
                             EnkiServer enkiServer,
                             NabuConnectionEventSource nabuConnectionEventSource,
                             Injector injector) {
        this.config = config;
        this.validator = validator;
        this.esClient = esClient;
        this.esEventSource = esEventSource;
        this.enkiServer = enkiServer;
        this.nabuConnectionEventSource = nabuConnectionEventSource;
        this.injector = injector;

        this.cnxnListener = new CnxnListener();

        this.$assignmentLock = new byte[0];
        this.$unclaimedAssignments = Sets.newConcurrentHashSet();
        this.confirmedWorkers = Sets.newConcurrentHashSet();
        this.needsRebalance = new AtomicBoolean(false);
    }


    @Override
    public void start() throws ComponentException {
        esEventSource.addNabuESEventListener(this);
        nabuConnectionEventSource.addNabuConnectionListener(cnxnListener);

        validator.getShardCountCache().values().forEach(esksp -> {
            String topic   = esksp.getTopicName();
            int partitions = esksp.getPartitions();

            for(int i = 0; i < partitions; i++) {
                $unclaimedAssignments.add(new TopicPartition(topic, i));
            }
        });
    }

    private void haltAndCatchFire() {
        Thread t = new Thread(() -> { this.shutdown(); injector.getInstance(Enki.class).shutdown(); });
        t.setName("WorkerCoordinator-HCF");
        t.start();
    }

    @Override
    public void shutdown() throws ComponentException {
        esEventSource.removeNabuESEventListener(this);
        nabuConnectionEventSource.addNabuConnectionListener(cnxnListener);
    }

    private void rebalanceAssignments() {
        synchronized ($assignmentLock) {
            List<AssignmentDelta<AssignableNabu, TopicPartition>> deltas;

            // build a list of deltas...
            try {
                AssignmentBalancer<AssignableNabu, TopicPartition> balancer = new AssignmentBalancer<>();

                for(AssignableNabu worker : confirmedWorkers) {
                    balancer.addWorker(worker, worker.getAssignments());
                }

                deltas = balancer.balance(ImmutableSet.of());
            } catch(AssertionError | Exception e) {
                logger.error("[FATAL] something went wrong when rebalancing the workload", e);
                logger.error("[FATAL] halting and catching fire.");
                haltAndCatchFire();
            }

            Set<TopicPartition> unassignedTasks = Sets.newConcurrentHashSet();
        }
    }

    @Override
    public void onNabuESEvent(NabuESEvent event) {
//        logger.info("{}", event);
        switch(event.getType()){
            case ENKI_JOINED:
                break;
            case ENKI_PARTED:
                break;
            case NABU_JOINED:
                break;
            case NABU_PARTED:
                break;
        }
    }

    private static class AssignableNabu {
        private final @Getter String esNodeName;
        private final @Getter NabuConnection cnxn;
        private @Getter @Setter Set<TopicPartition> assignments;

        AssignableNabu(String esNodeName, NabuConnection connection) {
            this.esNodeName = esNodeName;
            this.cnxn = connection;
            this.assignments = ImmutableSet.of();
        }
    }

    private class CnxnListener implements NabuConnectionListener {
        @Override
        public boolean onNewNabuConnection(NabuConnection cnxn) { return true; }

        @Override
        public boolean onNabuLeaving(NabuConnection cnxn, boolean serverInitiated) { return true; }

        @Override
        public boolean onNabuDisconnected(NabuConnection cnxn, EnkiConnection.DisconnectCause cause, boolean wasAcked) { return true; }

        @Override
        public boolean onPacketDispatched(NabuConnection cnxn, EnkiPacket packet, CompletableFuture<EnkiPacket> future) { return true; }

        @Override
        public boolean onPacketReceived(NabuConnection cnxn, EnkiPacket packet) { return true; }
    }
}
