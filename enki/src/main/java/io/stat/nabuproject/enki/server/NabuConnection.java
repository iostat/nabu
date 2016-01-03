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

    /**
     * A pretty, readable identifer this NabuConnection, preferably IP:PORT.
     * @return
     */
    String prettyName();

    /**
     * Respond with a NAK to a packet.
     * @param packet the packet to NAK
     */
    void nak(EnkiPacket packet);

    /**
     * Respond with an ACK to a packet.
     * @param packet the packet to ACK
     */
    void ack(EnkiPacket packet);
}
