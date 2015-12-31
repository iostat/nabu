package io.stat.nabuproject.core.elasticsearch.event;

/**
 * Something which listens for any events specified in {@link NabuESEvent.Type}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface NabuESEventListener {
    /**
     * Called when a {@link NabuESEvent} occurs.
     * @param event the event that occurred.
     */
    void onNabuESEvent(NabuESEvent event);
}
