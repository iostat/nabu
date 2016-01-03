package io.stat.nabuproject.core.enkiprotocol.dispatch;

import io.stat.nabuproject.core.enkiprotocol.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.EnkiSourcedConfigKeys;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Map;

/**
 * Something which can receive Enki protocol events.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface EnkiClientEventListener {
    /**
     * A callback that is sent when Enki sends new configuration data.
     * @param enki a high-level interface to the connected enki
     * @param config the config map that was received.
     * @return whether or not this callback ran successfully.
     * @see EnkiSourcedConfigKeys for the kind of keys you would expect to find in config.
     */
    default boolean onConfigurationReceived(EnkiConnection enki, Map<String, Serializable> config) {
        return true;
    }

    /**
     * A callback that is sent when Enki assigns a topic and partition to write in a throttled manner.
     * @param enki a high-level interface to the connected enki.
     * @param topicPartition a Kafka {@link TopicPartition} that was assigned to this Nabu by Enki
     * @return whether or not this callback ran successfully
     */
    default boolean onTaskAssigned(EnkiConnection enki, TopicPartition topicPartition) {
        return true;
    }

    /**
     * A callback that is sent when Enki requests that this Nabu stop performing throttled write operations
     * for a specific Kafka {@link TopicPartition}
     * @param enki a high-level interface to the connected Enki
     * @param topicPartition the Kafka {@link TopicPartition} that this Nabu should stop throttled-writing
     * @return whether or not this callback ran successfully
     */
    default boolean onTaskUnassigned(EnkiConnection enki, TopicPartition topicPartition) {
        return true;
    }

    /**
     * Called when this Nabu has disconnected from Enki
     * @param enki a high-level interface to the connected Enki. Not that it may very well be useless as it is no longer connected to anything.
     * @param wasLeaving whether or not this connection was terminated gracefully using an {@link io.stat.nabuproject.core.enkiprotocol.packet.EnkiLeave}
     *                   packet that was sent by either the server or by the client
     * @param serverInitiated true if it were Enki that requested the leave, false if it was this Nabu that requested the leave
     * @param wasAcked whether or not the LEAVE request, if present, was acknowledged by the other party
     * @return whether or not this callback ran successfully.
     */
    default boolean onConnectionLost(EnkiConnection enki, boolean wasLeaving, boolean serverInitiated, boolean wasAcked) {
        return true;
    }
}