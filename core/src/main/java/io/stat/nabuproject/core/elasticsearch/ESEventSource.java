package io.stat.nabuproject.core.elasticsearch;

import io.stat.nabuproject.core.elasticsearch.event.NabuESEventListener;

/**
 * Something which can accept {@link NabuESEventListener} listeners
 * and call their callbacks when a {@link io.stat.nabuproject.core.elasticsearch.event.NabuESEvent} comes in
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ESEventSource {
    /**
     * Register a {@link NabuESEventListener} to receive Nabu-related elasticsearch cluster events.
     * @param listener the {@link NabuESEventListener} to register
     */
    void addNabuESEventListener(NabuESEventListener listener);

    /**
     * Deregister a {@link NabuESEventListener} from receiving Nabu-related elasticsearch cluster events.
     * @param listener the {@link NabuESEventListener} to deregister
     */
    void removeNabuESEventListener(NabuESEventListener listener);
}
