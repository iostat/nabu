package io.stat.nabuproject.enki.leader;

/**
 * Something which can respond to Enki leader-election related events
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface LeaderEventListener {
    /**
     * Called when this instance becomes the Enki master.
     * @return true if the callback completed successfully, false otherwise.
     */
    boolean onSelfElected();

    /**
     * Called when a new leader was elected, but it was not this instance.
     * @param newLeaderAddress the Enki server address of the new leader
     * @param newLeaderPort the Enki server port of the new leader
     * @return true if the callback completed successfully, false otherwise
     */
    boolean onOtherElected(String newLeaderAddress, int newLeaderPort);
}
