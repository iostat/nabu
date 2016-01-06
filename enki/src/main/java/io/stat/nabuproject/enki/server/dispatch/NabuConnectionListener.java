package io.stat.nabuproject.enki.server.dispatch;

import io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.enki.server.NabuConnection;

import java.util.concurrent.CompletableFuture;

/**
 * Something which can be notified of new Nabu connections (to Enki).
 * Ideally, should do nothing that blocks.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface NabuConnectionListener {
    /**
     * Callback when a new connection is formed.
     * Ideally should not do anything that blocks.
     *
     * @param cnxn a high-level connection implementation that implements the Enki protocol, and can have further events
     *             registered against it
     * @return whether or not this callback ran successfully.
     */
    boolean onNewNabuConnection(NabuConnection cnxn);

    /**
     * Called when a Nabu is leaving on peaceable terms.
     * If it's server initiated, that means the server sent a LEAVE
     * that was ACK'd.
     *
     * If it's client initiated, this implies that the client has already unassigned its throttled
     * writers.
     *
     * @param cnxn the NabuConnection that's leaving.
     * @param serverInitiated true if this leave was server initated, false if it was client initiated.
     * @return whether or not this callback ran successfully.
     */
    boolean onNabuLeaving(NabuConnection cnxn, boolean serverInitiated);

    /**
     * Callback when the socket has closed and the connection is
     * basically useless.
     * @param cnxn the NabuConnection that left
     * @param cause the cause of the of disconnect
     * @param wasAcked true if the leave request was acked by the client (if server-initiated),
     *                 true if the client-inited leave request was acked by the server (and why wouldn't it be).
     *                 false otherwise
     * @return whether or not this callback ran successfully.
     */
    boolean onNabuDisconnected(NabuConnection cnxn, EnkiConnection.DisconnectCause cause, boolean wasAcked);

    /**
     * Called when a packet is dispatched. This includes heartbeats, etc.
     * @param cnxn the connection that the packet was sent to.
     * @param packet the packet that was sent.
     * @param future the future associated with this packet.
     * @return whether or not this callback ran successfully
     */
    boolean onPacketDispatched(NabuConnection cnxn, EnkiPacket packet, CompletableFuture<EnkiPacket> future);

    /**
     * Called when a packet is received. This includes ACKs, etc.
     * @param cnxn the connection it was received on
     * @param packet the packed that was received.
     * @return whether or not this callback ran successfullyw
     */
    boolean onPacketReceived(NabuConnection cnxn, EnkiPacket packet);
}
