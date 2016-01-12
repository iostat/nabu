package io.stat.nabuproject.core.enkiprotocol.dispatch;

import io.stat.nabuproject.core.enkiprotocol.EnkiSourcedConfigKeys;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAssign;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiConfigure;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiUnassign;
import io.stat.nabuproject.core.net.AddressPort;
import org.apache.kafka.common.TopicPartition;

/**
 * Something which can receive Enki protocol events.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface EnkiClientEventListener {

    /**
     * A callback that is called when the connection was redirected
     * The connection is still open at this point, but the server
     * is expecting an ACK.
     * @param sequence the sequence number of the associated REDIRECT packet
     * @param redirectedTo where this client was redirected to
     * @return whether or not this callback ran successfully.
     */
    default boolean onRedirected(EnkiConnection enki, long sequence, AddressPort redirectedTo) {
        return true;
    }

    /**
     * A callback that is sent when Enki sends new configuration data.
     * @param enki a high-level interface to the connected enki
     * @param packet the EnkiConfigure packet that was received
     * @return whether or not this callback ran successfully.
     * @see EnkiSourcedConfigKeys for the kind of keys you would expect to find in config.
     */
    default boolean onConfigurationReceived(EnkiConnection enki, EnkiConfigure packet) {
        return true;
    }

    /**
     * A callback that is sent when Enki assigns a topic and partition to write in a throttled manner.
     * @param enki a high-level interface to the connected enki.
     * @param packet the EnkiAssign packet that was sent to this Nabu
     * @return whether or not this callback ran successfully
     */
    default boolean onTaskAssigned(EnkiConnection enki, EnkiAssign packet) {
        return true;
    }

    /**
     * A callback that is sent when Enki requests that this Nabu stop performing throttled write operations
     * for a specific Kafka {@link TopicPartition}
     * @param enki a high-level interface to the connected Enki
     * @param packet the EnkiUnassign packet that was sent to this Nabu
     * @return whether or not this callback ran successfully
     */
    default boolean onTaskUnassigned(EnkiConnection enki, EnkiUnassign packet) {
        return true;
    }

    /**
     * Called when this Nabu has disconnected from Enki
     * @param enki a high-level interface to the connected Enki. Not that it may very well be useless as it is no longer connected to anything.
     * @param reason the {@link io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection.DisconnectCause} for this connection loss.
     * @param wasAcked whether or not the LEAVE request, if present, was acknowledged by the other party
     * @return whether or not this callback ran successfully.
     */
    default boolean onConnectionLost(EnkiConnection enki, EnkiConnection.DisconnectCause reason, boolean wasAcked) {
        return true;
    }
}
