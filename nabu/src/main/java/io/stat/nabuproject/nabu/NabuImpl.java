package io.stat.nabuproject.nabu;

import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.ComponentStarter;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiClient;
import io.stat.nabuproject.nabu.server.NabuServer;
import lombok.AllArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

/**
 * The actual Nabu core implementation.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@AllArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
class NabuImpl extends Nabu {
    private final NabuConfig config;
    private final ESClient esClient;
    private final EnkiClient enkiClient;
    private final NabuServer nabuServer;
    private final ComponentStarter componentStarter;

    @Override @Synchronized
    public void start() throws ComponentException {
        logger.info("nabu.env is set to: {}", config.getEnv());
        logger.info("nabu.es.path.home is set to: {}", config.getESHome());
        logger.info("nabu.es.cluster.name is set to: {}", config.getESClusterName());
        logger.info("ES HTTP is {}", config.isESHTTPEnabled() ? "enabled" : "disabled");
        if(config.isESHTTPEnabled()) {
            logger.info("ES HTTP Port is set to: {}", config.getESHTTPPort());
        }

        componentStarter.registerComponents(
                config,
                esClient,
                enkiClient,
                nabuServer);

        componentStarter.start();

        logger.info("Nabu is up like Donald Trump!");
    }

    @Override @Synchronized
    public void shutdown() throws ComponentException {
        componentStarter.shutdown();
    }
}
