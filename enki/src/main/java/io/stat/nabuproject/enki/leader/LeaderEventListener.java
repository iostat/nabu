package io.stat.nabuproject.enki.leader;

import io.stat.nabuproject.core.net.AddressPort;

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
     * @param leaderID the cluster-wide identifier of the new leader (generally its ES node name)
     * @param leaderAP the {@link AddressPort} of the new leader.
     * @return true if the callback completed successfully, false otherwise
     */
    boolean onOtherElected(String leaderID, AddressPort leaderAP);
}
