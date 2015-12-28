package io.stat.nabuproject.enki;

import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.ComponentStarter;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.enki.server.EnkiServer;
import lombok.AllArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by io on 12/28/15. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@AllArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
class EnkiImpl implements Enki {
    private final EnkiConfig config;
    private final ESClient esClient;
    private final EnkiServer enkiServer;

    private final ComponentStarter componentStarter;

    @Override @Synchronized
    public void start() throws ComponentException {
        componentStarter.start();
        componentStarter.registerComponents(
                this.config,
                this.esClient,
                this.enkiServer);

        logger.info("Enki is ready.");
    }

    @Override @Synchronized
    public void shutdown() throws ComponentException {
        componentStarter.shutdown();
        logger.info("Enki is done here.");
    }
}