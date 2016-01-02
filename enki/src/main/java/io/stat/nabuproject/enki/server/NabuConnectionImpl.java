package io.stat.nabuproject.enki.server;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAck;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiHeartbeat;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiLeave;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.net.ConnectionLostException;
import io.stat.nabuproject.core.net.NodeLeavingException;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a Nabu instance which has connected to the
 * Enki server.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class NabuConnectionImpl implements NabuConnection {
    private static final AttributeKey<Long> LAST_SEQUENCE = AttributeKey.newInstance("last_heartbeat");
    private final ChannelHandlerContext context;
    private NabuConnectionListener connectionListener;
    private final Map<Long, CompletableFuture<EnkiPacket>> promises;

    private final AtomicLong lastOutgoingSequence;
    private final AtomicLong lastIncomingSequence;

    private final AtomicLong lastHeartbeat;
    private final AtomicInteger missedHeartbeats;

    private final AtomicBoolean waitingForHeartbeat;
    private final AtomicBoolean isDisconnecting;

    private final AtomicBoolean wasLeavePacketSent;
    private final AtomicBoolean wasLeaveServerInitiated;
    private final AtomicBoolean leaveAcknowledged;

    private final TimerTask heartbeatTask;
    private final TimerTask leaveEnforcerTask;

    private final Timer heartbeatTimer;
    private final Timer leaveEnforcerTimer;

    public NabuConnectionImpl(ChannelHandlerContext context, NabuConnectionListener listener) {
        this.context = context;
        connectionListener = listener;
        this.context.attr(LAST_SEQUENCE).set(0L);
        logger.info("New NabuConnectionImpl({}) for {}", this, context);

        this.lastOutgoingSequence = new AtomicLong(0L);
        this.lastIncomingSequence = new AtomicLong(0L);

        this.missedHeartbeats     = new AtomicInteger(0);

        this.lastHeartbeat = new AtomicLong(0L);

        this.waitingForHeartbeat = new AtomicBoolean(false);
        this.isDisconnecting     = new AtomicBoolean(false);

        this.wasLeavePacketSent = new AtomicBoolean(false);
        this.wasLeaveServerInitiated = new AtomicBoolean(false);
        this.leaveAcknowledged   = new AtomicBoolean(false);

        this.promises = Maps.newConcurrentMap();

        this.heartbeatTimer = new Timer(String.format("NabuConnection-Heartbeat-%s", prettifyRemoteAddress()));
        this.leaveEnforcerTimer = new Timer(String.format("NabuConnection-LeaveEnforcer-%s", prettifyRemoteAddress()));

        this.heartbeatTask = this.new HeartbeatTask();
        this.leaveEnforcerTask = this.new LeaveEnforcerTask();

        // todo: heartbeat timeouts should be configurable.
        this.heartbeatTimer.scheduleAtFixedRate(this.heartbeatTask, 0, 3000);

        listener.onNewNabuConnection(this);
    }

    private CompletableFuture<EnkiPacket> dispatchHeartbeat() {
        return dispatchPacket(new EnkiHeartbeat(assignSequence()));
    }
    private CompletableFuture<EnkiPacket> dispatchKick() { return dispatchPacket(new EnkiLeave(assignSequence())); }
    private CompletableFuture<EnkiPacket> dispatchAck(long sequence) { return dispatchPacket(new EnkiAck(sequence)); }

    private CompletableFuture<EnkiPacket> dispatchPacket(EnkiPacket packet) {
        CompletableFuture<EnkiPacket> future = new CompletableFuture<>();
        connectionListener.onPacketDispatched(this, packet, future);
        if(!isDisconnecting.get()) {
            promises.put(packet.getSequenceNumber(), future);
            context.writeAndFlush(packet);
        } else if(packet.getType() == EnkiPacket.Type.LEAVE) {
            promises.put(packet.getSequenceNumber(), future);
            context.writeAndFlush(packet);
        } else {
            future.completeExceptionally(
                new NodeLeavingException(
                        "Cannot dispatch the packet as" +
                        " this node is disconnecting from the cluster"));
        }
        return future;
    }

    /**
     * Assigns the next sequence number.
     * @return the next sequence number.
     */
    private long assignSequence() {
        return lastOutgoingSequence.incrementAndGet();
    }

    @Override
    @Synchronized
    public void kick() {
        logger.info("kick called on {}", this);

        nabuLeaving(true);

        // todo: leave enforcer timeout should be configurable.
        leaveEnforcerTimer.schedule(leaveEnforcerTask, 5000);

        dispatchKick().whenCompleteAsync((packet, exception) -> {
            if(exception != null && !(exception instanceof ConnectionLostException)) {
                logger.error("Dispatch kick promise fulfilled with unexpected exception: ", exception);
            } else if (exception != null && exception instanceof ConnectionLostException) {
                logger.info("The connection to Nabu was terminated while waiting for a response to a LEAVE.");
            } else {
                this.leaveAcknowledged.set(true);
                context.close();
            }
        });
    }

    /**
     * common functionality for when kick() is called vs. client notifies that
     * it's leaving.
     * @param serverInitiated true for kick(), false for client-initiated
     */
    private void nabuLeaving(boolean serverInitiated) {
        isDisconnecting.set(true);
        heartbeatTimer.cancel();
        int outstandingPromises = promises.size();
        if(outstandingPromises != 0) {
            logger.warn("{} has {} outstanding promises! NodeLeavingEx'ing them all!", this, outstandingPromises);
            promises.forEach((seq, promise) ->
                    promise.completeExceptionally(new NodeLeavingException(
                            "The Nabu node is leaving the cluster " +
                                    "and will not be able to respond to this request.")));
        }

        connectionListener.onNabuLeaving(this, true);

        wasLeavePacketSent.set(true);
        wasLeaveServerInitiated.set(serverInitiated);
    }

    @Override @Synchronized
    public void onDisconnected() {
        int outstandingPromises = promises.size();
        if(outstandingPromises != 0) {
            logger.warn("{} has {} outstanding promises! ConnectionLostEx'ing them all!", this, outstandingPromises);
            promises.forEach((seq, promise) ->
                promise.completeExceptionally(
                    new ConnectionLostException(
                            "The connection to the Nabu node " +
                            "has been lost")));
        }

        heartbeatTimer.cancel();

        // todo: dispatch to listeners that the client has disconnected.
        // todo: for whatever reason (whether kicked or just connection lost)
        connectionListener.onNabuDisconnected(this, wasLeavePacketSent.get(), wasLeaveServerInitiated.get(), leaveAcknowledged.get());
    }

    @Override @Synchronized
    public void onPacketReceived(EnkiPacket packet) {
        connectionListener.onPacketReceived(this, packet);
        long sequence = packet.getSequenceNumber();
        CompletableFuture<EnkiPacket> f = promises.getOrDefault(sequence, null);
        if(f != null) {
            f.complete(packet);
            promises.remove(sequence);
        } else {
            if (sequence == lastIncomingSequence.get() + 1L) {
                logger.info("Wow! received a fantastic packet: {}", packet);

                if(packet.getType() == EnkiPacket.Type.LEAVE) {
                    nabuLeaving(false);
                    dispatchAck(packet.getSequenceNumber());
                } else {
                    // todo: client shouldn't be sending anything else. what to do?
                    logger.error("RECEIVED A PACKET THAT CLIENTS SHOULDNT BE SENDING :: {}", packet);
                }
            } else {
                logger.error("RECEIVED AN UNEXPECTED PACKET (sequence out of order) :: {}", packet);
                // todo: kick the client?
            }
        }
    }

    @Override @Synchronized
    public String toString() {
        MoreObjects.ToStringHelper tsh = MoreObjects.toStringHelper(this);
        tsh.add("addr", prettifyRemoteAddress())
           .add("isDisconnecting", isDisconnecting.get())
           .add("lastOutgoingSequence", lastOutgoingSequence.get());

        if(waitingForHeartbeat.get()) {
            tsh.add("waitingForHeartbeat", true);
            tsh.add("missedHeartbeats", missedHeartbeats.get());
        }

        tsh.add("outstandingPromises", promises.size());

        return tsh.toString();
    }

    private String prettifyRemoteAddress() {
        InetSocketAddress remoteAddress = ((InetSocketAddress) context.channel().remoteAddress());
        return remoteAddress.getAddress() + ":" + remoteAddress.getPort();
    }

    /**
     * A task that sends out heartbeats, and boots the client if too many are missed.
     */
    private class HeartbeatTask extends TimerTask {
        @Override
        public void run() {
            if(waitingForHeartbeat.get()) {
                int missedHeartbeats = NabuConnectionImpl.this.missedHeartbeats.incrementAndGet();
                logger.warn("{} missed heartbeat(s) since last heartbeat...", missedHeartbeats);

                // todo: this should be configurable.
                if(missedHeartbeats >= 5) {
                    logger.error("Nabu node has missed {} heartbeats now!", missedHeartbeats);
                    // todo: force deallocate and kick from cluster.
                    NabuConnectionImpl.this.kick();
                }
            } else {
                waitingForHeartbeat.set(true);
                CompletableFuture<EnkiPacket> f = NabuConnectionImpl.this.dispatchHeartbeat();
                f.whenCompleteAsync((enkiPacket, throwable) -> {
                    if(throwable == null) {
                        NabuConnectionImpl.this.lastHeartbeat.set(System.currentTimeMillis());
                        missedHeartbeats.set(0);
                    } else {
                        if(throwable instanceof ConnectionLostException || throwable instanceof NodeLeavingException) {
                            logger.debug("Heartbeat future completed with CnxnLost/NodeLeaving", throwable);
                        } else {
                            logger.warn("Heartbeat future was completed with an exception: {}", throwable);
                        }
                    }

                    waitingForHeartbeat.set(false);
                });
            }
        }
    }

    /**
     * A task that gets started when the Enki server initiates a leave request, that ensures that
     * the client has disconnected.
     */
    private class LeaveEnforcerTask extends TimerTask {
        @Override
        public void run() {
            if(!NabuConnectionImpl.this.leaveAcknowledged.get()) {
                logger.error("Server-initiated leave request was NOT acknowledged..");
                logger.error("Forcibly closing the connection to {}. " +
                        "You may want to kill the node to ensure data doesn't get re-written!",
                        NabuConnectionImpl.this);
            } else {
                logger.debug("No need to enforce leave request for {}", NabuConnectionImpl.this);
            }
        }
    }
}
