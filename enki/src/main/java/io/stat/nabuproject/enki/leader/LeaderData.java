package io.stat.nabuproject.enki.leader;

import io.stat.nabuproject.Version;
import io.stat.nabuproject.core.net.AddressPort;

import java.io.Serializable;

/**
 * Enki node metadata specifically related to leader election.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class LeaderData implements Serializable {
    /**
     * Whether or not the leader that this data describes is acceptable for
     * use in leader election. For instance, a basic implementation would
     * compare that the version of Enki that the leader describes is the same
     * version that this instance is running.
     * @return
     */
    public boolean isAcceptable() {
        return getVersion().equals(Version.VERSION);
    }

    /**
     * Get the version of Enki that the leader is running
     * @return the version of Enki that the leader is running.
     */
    public abstract String getVersion();

    /**
     * The identifier that the Enki this leader data supplied.
     * The canonical implementation supplies its ElasticSearch node name
     * @return the node's identifier.
     */
    public abstract String getNodeIdentifier();

    /**
     * The AddressPort to use to connect to the Enki server
     * @return the AddressPort that the describe Enki runs its service on.
     */
    public abstract AddressPort getAddressPort();

    /**
     * If this leader has any sort of priority attached to it, such as
     * a ZNode sequence number, etc.
     */
    public abstract long getPriority();
}
