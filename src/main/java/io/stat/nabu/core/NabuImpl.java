package io.stat.nabu.core;

import com.google.inject.Inject;
import io.stat.nabu.config.ConfigurationProvider;
import io.stat.nabu.elasticsearch.ESClient;
import io.stat.nabu.server.NabuServer;
import lombok.Synchronized;
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
        logger.info("nabu.es.cluster.name is set to: {}", config.getESClusterName());
        logger.info("ES HTTP is {}", config.isESHTTPEnabled() ? "enabled" : "disabled");
        if(config.isESHTTPEnabled()) {
            logger.info("ES HTTP Port is set to: {}", config.getESHTTPPort());
        }
    }

    @Override @Synchronized
    public void start() throws ComponentException {
        ((Component)config)._dispatchStart();
        ((Component)esClient)._dispatchStart();

        ((Component)nabuServer)._dispatchStart();

        logger.info("Nabu is up like Donald Trump!");
    }

    @Override @Synchronized
    public void shutdown() throws ComponentException {
        ((Component)config)._dispatchShutdown();
        ((Component)esClient)._dispatchShutdown();

        ((Component)nabuServer)._dispatchShutdown();
    }
}
