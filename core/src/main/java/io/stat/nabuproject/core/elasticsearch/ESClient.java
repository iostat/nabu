package io.stat.nabuproject.core.elasticsearch;

import com.google.common.collect.Sets;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEvent;
import io.stat.nabuproject.core.elasticsearch.event.NabuESEventListener;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.elasticsearch.client.Client;

import java.util.Set;

/**
 * A class which provides access to the elasticsearch cluster that is being operated upon.
 * This includes getting a client to query ES, as well as being able to provide events
 * that can be listened to. An ESClient is expected to dispatch events to listeners.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode(callSuper=true)
public abstract class ESClient extends Component {
    /**
     * The Set of {@link NabuESEventListener}s registered to this ESClient.
     */
    private @Getter(AccessLevel.PROTECTED) Set<NabuESEventListener> nabuESEventListeners;

    public ESClient() {
        this.nabuESEventListeners = Sets.newConcurrentHashSet();
    }

    /**
     * Returns an ElasticSearch Client that can query the cluster.
     * @return a {@link org.elasticsearch.client.Client} for the cluster that this client is connected to
     */
    public abstract Client getESClient();

    /**
     * Register a {@link NabuESEventListener} to receive Nabu-related elasticsearch cluster events.
     * @param listener a {@link NabuESEventListener}
     */
    public void registerNabuEventListener(NabuESEventListener listener) {
        nabuESEventListeners.add(listener);
    }

    /**
     * Used by implementors to dispatch a {@link NabuESEvent} to all registered listeners.
     * @param event the event to dispatch.
     */
    protected void dispatchNabuEsEvent(NabuESEvent event) {
        nabuESEventListeners.forEach(listener -> listener.onNabuESEvent(event));
    }
}
