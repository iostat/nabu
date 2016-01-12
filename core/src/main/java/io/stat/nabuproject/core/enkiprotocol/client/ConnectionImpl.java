package io.stat.nabuproject.core.enkiprotocol.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventListener;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAck;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAssign;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiConfigure;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiHeartbeat;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiLeave;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiNak;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiRedirect;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiUnassign;
import io.stat.nabuproject.core.net.AddressPort;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The implementation of the high-level Enki client.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class ConnectionImpl implements EnkiConnection {
    static final AttributeKey<Boolean> SUPPRESS_DISCONNECT_CALLBACK = AttributeKey.newInstance("SUPPRESS_DISCONNECT_CALLBACK");

    private final ChannelHandlerContext ctx;
    private final EnkiClient creator;
    private final EnkiClientEventListener toNotify;

    private final AtomicLong lastOutgoingSequence;
    private final AtomicLong lastIncomingSequence;
    private final AtomicLong sequenceNumberOfLeave;

    private final AtomicLong lastHeartbeatTimestamp;
    private final AtomicLong lastConfigTimestamp;

    private final AtomicBoolean isDisconnecting;

    private final AtomicBoolean wasLeavePacketSent;
    private final AtomicBoolean wasLeaveServerInitiated;
    private final AtomicBoolean wasLeaveAcknowledged;
    private final AtomicBoolean wasRedirected;

    private final CompletableFuture<ConnectionImpl> afterClientLeaveAcked;

    public ConnectionImpl(ChannelHandlerContext ctx,
                          EnkiClient creator,
                          EnkiClientEventListener toNotify) {
        this.ctx = ctx;
        this.creator = creator;
        this.toNotify = toNotify;

        lastIncomingSequence = new AtomicLong(0);
        lastOutgoingSequence = new AtomicLong(0);

        lastHeartbeatTimestamp = new AtomicLong(0);
        lastConfigTimestamp = new AtomicLong(0);

        sequenceNumberOfLeave = new AtomicLong(-1);

        isDisconnecting = new AtomicBoolean(false);
        wasLeavePacketSent = new AtomicBoolean(false);
        wasLeaveServerInitiated = new AtomicBoolean(false);
        wasLeaveAcknowledged = new AtomicBoolean(false);
        wasRedirected = new AtomicBoolean(false);

        afterClientLeaveAcked = new CompletableFuture<>();
    }

    @Override
    public void ack(long sequence) {
        dispatchPacket(new EnkiAck(sequence));
    }

    @Override
    public void nak(long sequence) {
        dispatchPacket(new EnkiNak(sequence));
    }

    public void onDisconnected() {
        DisconnectCause cause;
        if(wasLeavePacketSent.get()) {
            if(wasLeaveServerInitiated.get()) {
                cause = DisconnectCause.SERVER_LEAVE_REQUEST;
            } else {
                cause = DisconnectCause.CLIENT_LEAVE_REQUEST;
            }
        } else {
            cause = DisconnectCause.CONNECTION_RESET;
        }

        if(!ctx.attr(ConnectionImpl.SUPPRESS_DISCONNECT_CALLBACK).get()) {
            toNotify.onConnectionLost(this, cause, wasLeaveAcknowledged.get());
        }
    }

    public void onPacketReceived(EnkiPacket p) {
        lastIncomingSequence.set(p.getSequenceNumber());
        switch(p.getType()) {
            case HEARTBEAT:
                lastHeartbeatTimestamp.set(((EnkiHeartbeat) p).getTimestamp());
                dispatchPacket(new EnkiAck(p.getSequenceNumber()));
                break;
            case CONFIGURE:
                lastConfigTimestamp.set(System.currentTimeMillis());
                toNotify.onConfigurationReceived(this, ((EnkiConfigure)p));
                dispatchPacket(new EnkiAck(p.getSequenceNumber()));
                break;
            case LEAVE:
                // todo: LEAVE
                wasLeaveServerInitiated.set(true);
                wasLeavePacketSent.set(true);
                dispatchPacket(new EnkiAck(p.getSequenceNumber()));
                wasLeaveAcknowledged.set(true);
                break;
            case REDIRECT:
                AddressPort target = ((EnkiRedirect) p).getDestination();
                logger.info("RECV Redirect {}", target);
                wasLeaveServerInitiated.set(false);
                wasLeavePacketSent.set(false);
                wasLeaveAcknowledged.set(true);
                wasRedirected.set(true);
                creator.setRedirectionTarget(target);
                toNotify.onRedirected(this, p.getSequenceNumber(), target);
                ctx.attr(ConnectionImpl.SUPPRESS_DISCONNECT_CALLBACK).set(true);
                break;
            case ACK:
                if(p.getSequenceNumber() == sequenceNumberOfLeave.get()) {
                    wasLeaveAcknowledged.set(true);
                    afterClientLeaveAcked.complete(this);
                }
                break;
            case ASSIGN:
            case UNASSIGN:
                EnkiAssign packet = ((EnkiAssign) p);
                boolean isAssign = (p.getType() == EnkiPacket.Type.ASSIGN);

                if(isAssign) {
                    toNotify.onTaskAssigned(this, packet);
                } else {
                    toNotify.onTaskUnassigned(this, ((EnkiUnassign)packet));
                }
                break;
        }
    }

    @Synchronized
    @Override
    public void leaveGracefully() {
        isDisconnecting.set(true);
        EnkiLeave leave = new EnkiLeave(assignSequence());
        dispatchPacket(leave);
        sequenceNumberOfLeave.set(leave.getSequenceNumber());
        wasLeavePacketSent.set(true);
        wasLeaveServerInitiated.set(false);

        afterClientLeaveAcked.thenAcceptAsync(eci -> eci.killConnection(true));
    }

    private void killConnection(boolean suppressWarning) {
        if(!suppressWarning) {
            logger.warn("killConnection() called. This is going to be ugly.");
        }
        ctx.close();
    }

    @Override
    public void killConnection() {
        killConnection(false);
    }

    private void dispatchPacket(EnkiPacket p) {
        ctx.writeAndFlush(p);
    }

    private long assignSequence() {
        return lastOutgoingSequence.incrementAndGet();
    }

    @Override
    public String prettyName() {
        InetSocketAddress remoteAddress = ((InetSocketAddress) ctx.channel().remoteAddress());
        return remoteAddress.getAddress() + ":" + remoteAddress.getPort();
    }
}
