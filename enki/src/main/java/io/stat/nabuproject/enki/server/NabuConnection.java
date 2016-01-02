package io.stat.nabuproject.enki.server;

import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;

/**
 * Defines what is considered a high-level Nabu client.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface NabuConnection {
    /**
     * Send this client a LEAVE request, and try to ask it to leave amicably.
     */
    void kick();

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
}
