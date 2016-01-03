package io.stat.nabuproject.core.enkiprotocol;

/**
 * Something which can call the callbacks of {@link EnkiClientEventListener}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface EnkiClientEventSource {
    /**
     * Register an {@link EnkiClientEventListener} to receive callbacks
     * @param ecel the listener to register.
     */
    void addEnkiClientEventListener(EnkiClientEventListener ecel);

    /**
     * Stop sending callbacks to an {@link EnkiClientEventListener}.
     * @param ecel the listener to deregister
     */
    void removeEnkiClientEventListener(EnkiClientEventListener ecel);
}
