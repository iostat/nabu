package io.stat.nabuproject.enki;

import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.ComponentStarter;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.kafka.KafkaMetadataClient;
import io.stat.nabuproject.enki.integration.IntegrationSanityChecker;
import io.stat.nabuproject.enki.integration.WorkerCoordinator;
import io.stat.nabuproject.enki.leader.EnkiLeaderElector;
import io.stat.nabuproject.enki.server.EnkiServer;
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
class EnkiImpl implements Enki {
    private final EnkiConfig config;
    private final ESClient esClient;
    private final EnkiServer enkiServer;
    private final KafkaMetadataClient metadataClient;
    private final IntegrationSanityChecker sanityChecker;
    private final WorkerCoordinator workerCoordinator;
    private final EnkiLeaderElector leaderElector;

    private final ComponentStarter componentStarter;

    @Override @Synchronized
    public void start() throws ComponentException {
        componentStarter.registerComponents(
                this.config,
                this.esClient,
                this.metadataClient,
                this.sanityChecker,
                this.leaderElector,
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
