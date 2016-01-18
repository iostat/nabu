package io.stat.nabuproject.enki.server;

import io.stat.nabuproject.core.enkiprotocol.EnkiPacketConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.CompletableFuture;

/**
 * Defines what is considered a high-level Nabu client.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface NabuConnection extends EnkiPacketConnection {
    /**
     * Send this client a LEAVE request, and try to ask it to leave amicably.
     */
    void leaveGracefully();

    /**
     * Used by the ServerIO backend to inform the connection that its channel has closed.
     */
    void onDisconnected();

    /**
     * Used by the ServerIO backend to inform the connection that it received a packet.
     * (Any packet, ACK, LEAVE, etc)
     * @param packet the packet that was received.
     */
    void onPacketReceived(EnkiPacket packet);

    /**
     * Tell the client to start working on a Kafka TopicPartition assignment.
     * @param assignment the TopicPartition to begin consuming
     * @return a future that can be listened to which will be fullfilled with the Nabu's response to this request
     */
    CompletableFuture<EnkiPacket> sendAssign(TopicPartition assignment);

    /**
     * Tell the connected Nabu to stop consuming a Kafka TopicPartition
     * @param assignment the TopicPartition to stop consuming
     * @return a future that can be listened to which will be fullfilled with the Nabu's response to this request
     */
    CompletableFuture<EnkiPacket> sendUnassign(TopicPartition assignment);

    /**
     * Dispatches a fresh configuration packet to the connection.
     */
    void refreshConfiguration();
}
