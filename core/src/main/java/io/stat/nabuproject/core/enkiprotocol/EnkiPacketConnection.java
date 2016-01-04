package io.stat.nabuproject.core.enkiprotocol;

import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;

/**
 * Something which can send EnkiPackets.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface EnkiPacketConnection {
    /**
     * Reply to an {@link EnkiPacket} stating that it's been acknowledged
     * @param p the packet to ACK
     */
    void ack(EnkiPacket p);

    /**
     * Reply to an {@link EnkiPacket} stating that it has NOT been acknowledged
     * @param p the packet to NAK
     */
    void nak(EnkiPacket p);

    /**
     * Return a short, human-readable description of this connection.
     * @return what the description above says.
     */
    String prettyName();

    /**
     * Try to leave gracefully.
     */
    void leaveGracefully();

    /**
     * Kill the connection immediately.
     */
    void killConnection();
}
