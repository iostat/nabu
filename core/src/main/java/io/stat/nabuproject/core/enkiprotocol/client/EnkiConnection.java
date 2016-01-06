package io.stat.nabuproject.core.enkiprotocol.client;

import io.stat.nabuproject.core.enkiprotocol.EnkiPacketConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;

/**
 * Created by io on 1/2/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface EnkiConnection extends EnkiPacketConnection {
    void ack(long sequence);
    void nak(long sequence);

    default void ack(EnkiPacket p) { ack(p.getSequenceNumber()); }
    default void nak(EnkiPacket p) { nak(p.getSequenceNumber()); }

    enum DisconnectCause {
        REDIRECT,
        SERVER_LEAVE_REQUEST,
        CLIENT_LEAVE_REQUEST,
        CONNECTION_RESET
    };
}
