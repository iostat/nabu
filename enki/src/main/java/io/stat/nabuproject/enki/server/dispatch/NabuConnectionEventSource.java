package io.stat.nabuproject.enki.server.dispatch;

/**
 * Something which can call the callbacks of {@link NabuConnectionListener}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface NabuConnectionEventSource {
    /**
     * Register a consumer of {@link NabuConnectionListener} callbacks.
     * @param listener the listener to register
     */
    void addNabuConnectionListener(NabuConnectionListener listener);

    /**
     * Deregister a consumer of {@link NabuConnectionListener} callbacks.
     * @param listener the listener to deregister
     */
    void removeNabuConnectionListener(NabuConnectionListener listener);
}
