package io.stat.nabuproject.core.elasticsearch.event;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;

/**
 * Something which listens for any events specified in {@link NabuESEvent.Type}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@FunctionalInterface
public interface NabuESEventListener {
    /**
     * Called when a {@link NabuESEvent} occurs.
     * @param event the event that occurred.
     * @return true on success handling the callback, false on failure.
     */
    boolean onNabuESEvent(NabuESEvent event);

    /**
     * Callback on when the elasticsearch cluster's color has changed.
     */
    default boolean onClusterColorChange(ClusterHealthStatus color) {
        return true;
    }
}
