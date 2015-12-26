package io.stat.nabu.core;

import com.google.inject.Inject;
import io.stat.nabu.config.ConfigurationProvider;
import io.stat.nabu.elasticsearch.ESClient;
import io.stat.nabu.server.NabuServer;
import lombok.extern.slf4j.Slf4j;

/**
 * The actual Nabu core implementation.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class NabuImpl extends Component implements Nabu {
    private final ConfigurationProvider config;
    private final ESClient esClient;
    private final NabuServer nabuServer;

    @Inject
    NabuImpl(ConfigurationProvider config,
             ESClient esClient,
             NabuServer nabuServer) {
        this.config = config;
        this.esClient = esClient;
        this.nabuServer = nabuServer;

        logger.info("nabu.env is set to: {}", config.getEnv());
        logger.info("nabu.es.path.home is set to: {}", config.getESHome());
        logger.info("ES HTTP is {}", config.isESHTTPEnabled() ? "enabled" : "disabled");
        if(config.isESHTTPEnabled()) {
            logger.info("ES HTTP Port is set to: {}", config.getESHTTPPort());
        }
    }

    @Override
    public void start() throws ComponentException {
        config.start();
        esClient.start();

        nabuServer.start();

        logger.info("Nabu is up like Donald Trump!");
    }

    @Override
    public void shutdown() throws ComponentException {
        config.shutdown();
        esClient.shutdown();

        nabuServer.shutdown();
    }
}
