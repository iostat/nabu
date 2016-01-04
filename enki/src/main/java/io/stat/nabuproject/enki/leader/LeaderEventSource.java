package io.stat.nabuproject.enki.leader;

/**
 * Something which can accept LeaderEventListeners and dispatch events to them
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface LeaderEventSource {
    /**
     * Register a {@link LeaderEventListener}. Callbacks will be dispatch asynchronously
     * @param listener the listener to register
     */
    void addLeaderEventListener(LeaderEventListener listener);

    /**
     * Deregister a {@link LeaderEventListener}
     * @param listener the lsitener to deregister
     */
    void removeLeaderEventListener(LeaderEventListener listener);

    /**
     * @return Whether or not this Enki is the current master.
     */
    boolean isLeader();
}
