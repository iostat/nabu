package io.stat.nabuproject.core.elasticsearch;

import com.google.common.collect.Sets;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEvent;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEventListener;
import io.stat.nabuproject.core.enkiprotocol.EnkiAddressProvider;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.Client;

import java.util.Set;

/**
 * A class which provides access to the elasticsearch cluster that is being operated upon.
 * This includes getting a client to query ES, as well as being able to provide events
 * that can be listened to. An ESClient is expected to dispatch events to listeners.
 *
 * Furthermore, since Enki discovery is performed against the ES cluster, an ESClient must
 * implement EnkiAddressProvider
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode(callSuper=true)
@Slf4j
public abstract class ESClient extends Component implements EnkiAddressProvider, ESEventSource {
    /**
     * The Set of {@link NabuESEventListener}s registered to this ESClient.
     */
    private @Getter(AccessLevel.PROTECTED) Set<NabuESEventListener> eSEventListeners;

    public ESClient() {
        this.eSEventListeners = Sets.newConcurrentHashSet();
    }

    /**
     * Returns an ElasticSearch Client that can query the cluster.
     * @return a {@link org.elasticsearch.client.Client} for the cluster that this client is connected to
     */
    public abstract Client getESClient();

    @Override
    public void addNabuESEventListener(NabuESEventListener listener) {
        logger.info("Registered NabuESEventListener {}", listener);
        eSEventListeners.add(listener);
    }

    @Override
    public void removeNabuESEventListener(NabuESEventListener listener) {
        eSEventListeners.remove(listener);
        logger.info("Unregistered NabuESEventListener {}", listener);
    }

    /**
     * Used by implementors to dispatch a {@link NabuESEvent} to all registered listeners.
     * @param event the event to dispatch.
     */
    protected void dispatchNabuEsEvent(NabuESEvent event) {
        eSEventListeners.forEach(listener -> listener.onNabuESEvent(event));
    }

    public abstract String getESIdentifier();
}
