package io.stat.nabuproject.enki.integration;

import com.google.inject.Inject;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.elasticsearch.ESEventSource;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEvent;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEventListener;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.server.EnkiServer;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionEventSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Listens to ElasticSearch for nodes joining and leaving, assigns work
 * as needed, etc.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor(onConstructor=@__(@Inject))
@Slf4j
public class WorkerCoordinator extends Component implements NabuESEventListener {
    private final ThrottlePolicyProvider config;
    private final ESClient esClient;
    private final ESEventSource esEventSource;
    private final EnkiServer enkiServer;
    private final NabuConnectionEventSource eventSource;

    @Override
    public void start() throws ComponentException {
        esEventSource.addNabuESEventListener(this);
    }

    @Override
    public void shutdown() throws ComponentException {
        esEventSource.removeNabuESEventListener(this);
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
}
