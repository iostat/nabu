package io.stat.nabuproject.enki;

import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.ComponentStarter;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.kafka.KafkaMetadataClient;
import io.stat.nabuproject.core.telemetry.TelemetryService;
import io.stat.nabuproject.enki.integration.ESKafkaValidator;
import io.stat.nabuproject.enki.integration.LeaderLivenessIntegrator;
import io.stat.nabuproject.enki.integration.WorkerCoordinator;
import io.stat.nabuproject.enki.leader.EnkiLeaderElector;
import io.stat.nabuproject.enki.server.EnkiServer;
import io.stat.nabuproject.enki.zookeeper.ZKClient;
import lombok.AllArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

/**
 * A Guice injectable implementation of Enki.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@AllArgsConstructor(onConstructor=@__(@Inject))
@Slf4j
class EnkiImpl extends Enki {
    private final EnkiConfig config;
    private final ESClient esClient;
    private final TelemetryService telemetry;
    private final ZKClient zkClient;
    private final EnkiServer enkiServer;
    private final KafkaMetadataClient metadataClient;
    private final ESKafkaValidator sanityChecker;
    private final EnkiLeaderElector leaderElector;
    private final LeaderLivenessIntegrator leaderLivenessIntegrator;
    private final WorkerCoordinator workerCoordinator;
    private final ComponentStarter componentStarter;

    @Override @Synchronized
    public void start() throws ComponentException {
        componentStarter.registerComponents(
                this.config,
                this.telemetry,
                this.esClient,
                this.zkClient,
                this.metadataClient,
                this.sanityChecker,
                this.leaderElector,
                this.leaderLivenessIntegrator,
                this.enkiServer,
                this.workerCoordinator);

        try {
            componentStarter.start();
            logger.info("Enki is ready.");
        } catch (ComponentException e) {
            logger.error("Failed to start!", e);
            shutdown();
        }
    }

    @Override @Synchronized
    public void shutdown() throws ComponentException {
        componentStarter.shutdown();
        logger.info("Enki is done here.");
    }
}
