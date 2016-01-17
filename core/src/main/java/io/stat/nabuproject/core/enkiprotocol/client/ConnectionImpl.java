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
import io.stat.nabuproject.core.util.concurrent.NamedThreadFactory;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    private static final NamedThreadFactory HEARTBEAT_ENFORCER_TASK_FACTORY = new NamedThreadFactory("EnkiClientHeartbeatEnforcer");
    private static final NamedThreadFactory LEAVE_ENFORCER_TASK_FACTORY = new NamedThreadFactory("EnkiClientLeaveEnforcer");

    /**
     * How much time may elapse between a heartbeat being received before the client
     * preemptively shuts itself down for safety.
     */
    public static final long MAX_HEARTBEAT_DISCREPANCY = 10000; // todo: tunable?

    /**
     * How much time may elapse between a heartbeat being received before the client
     * preemptively shuts itself down for safety.
     */
    public static final long MAX_HEARTBEAT_LEAVE_TIMEOUT = 3000; // todo: tunable?

    private final ChannelHandlerContext ctx;
    private final EnkiClient creator;
    private final EnkiClientEventListener toNotify;

    private final AtomicLong lastOutgoingSequence;
    private final AtomicLong lastIncomingSequence;
    private final AtomicLong sequenceNumberOfLeave;

    private final AtomicLong lastHeartbeatTimestamp;
    private final AtomicLong lastHeartbeatUpdatedAt;
    private final AtomicLong lastConfigTimestamp;

    private final AtomicBoolean isDisconnecting;

    private final AtomicBoolean wasLeavePacketSent;
    private final AtomicBoolean wasLeaveServerInitiated;
    private final AtomicBoolean wasLeaveAcknowledged;
    private final AtomicBoolean wasRedirected;

    private final CompletableFuture<ConnectionImpl> afterClientLeaveAcked;
    private final ScheduledExecutorService heartbeatEnforcerExecutor;
    private final ScheduledExecutorService heartbeatLeaveEnforcer;

    private final long createdOn;

    public ConnectionImpl(ChannelHandlerContext ctx,
                          EnkiClient creator,
                          EnkiClientEventListener toNotify) {
        this.ctx = ctx;
        this.creator = creator;
        this.toNotify = toNotify;

        lastIncomingSequence = new AtomicLong(0);
        lastOutgoingSequence = new AtomicLong(0);

        lastHeartbeatTimestamp = new AtomicLong(0);
        lastHeartbeatUpdatedAt = new AtomicLong(0);
        lastConfigTimestamp = new AtomicLong(0);

        sequenceNumberOfLeave = new AtomicLong(-1);

        isDisconnecting = new AtomicBoolean(false);
        wasLeavePacketSent = new AtomicBoolean(false);
        wasLeaveServerInitiated = new AtomicBoolean(false);
        wasLeaveAcknowledged = new AtomicBoolean(false);
        wasRedirected = new AtomicBoolean(false);

        afterClientLeaveAcked = new CompletableFuture<>();

        heartbeatEnforcerExecutor = Executors.newSingleThreadScheduledExecutor(HEARTBEAT_ENFORCER_TASK_FACTORY);
        heartbeatLeaveEnforcer = Executors.newSingleThreadScheduledExecutor(LEAVE_ENFORCER_TASK_FACTORY);

        createdOn = System.nanoTime();

        logger.info("ConnectionImpl created as {}", prettyName());
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
        heartbeatLeaveEnforcer.shutdownNow();
        heartbeatEnforcerExecutor.shutdownNow();

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
                lastHeartbeatUpdatedAt.set(System.currentTimeMillis());
                dispatchPacket(new EnkiAck(p.getSequenceNumber()));
                break;
            case CONFIGURE:
                // pre-seed the heartbeat monitor so as not to horrify it as soon as it starts
                lastHeartbeatUpdatedAt.set(System.currentTimeMillis());
                lastConfigTimestamp.set(System.currentTimeMillis());
                heartbeatEnforcerExecutor.scheduleAtFixedRate(this::heartbeatEnforcerTask, 1, 3, TimeUnit.SECONDS);
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

    private void heartbeatEnforcerTask() {
        long lastHeartbeat = lastHeartbeatTimestamp.get();
        long now = System.currentTimeMillis();
        long discrepancy = now - lastHeartbeat;

        if(Math.abs(discrepancy) >= MAX_HEARTBEAT_DISCREPANCY) {
            logger.error("Have not received a heartbeat in {} ms (max is {}). Disconnecting for safety.", discrepancy, MAX_HEARTBEAT_DISCREPANCY);
            leaveGracefully();
            heartbeatLeaveEnforcer.schedule(() -> {
                if(!wasLeaveAcknowledged.get()) {
                    logger.error("Heartbeat-discrepancy-related leave was not acknowledged after {} ms. Killing the connection violently", MAX_HEARTBEAT_LEAVE_TIMEOUT);
                    killConnection(true);
                }
            }, MAX_HEARTBEAT_LEAVE_TIMEOUT, TimeUnit.MILLISECONDS);
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
        InetSocketAddress local = ((InetSocketAddress) ctx.channel().localAddress());
        return local.getAddress() + ":" + local.getPort();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null) {
            return false;
        }

        if(!(obj instanceof ConnectionImpl)) {
            return false;
        }

        ConnectionImpl other = ((ConnectionImpl) obj);

        if(this.createdOn != other.createdOn) {
            return false;
        }

        if(ctx == null) {
            return other.ctx == null;
        } else {
            return this.ctx.equals(other.ctx);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(createdOn, ctx);
    }
}
