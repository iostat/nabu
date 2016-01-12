package io.stat.nabuproject.enki.server;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.stat.nabuproject.core.enkiprotocol.EnkiSourcedConfigKeys;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAck;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAssign;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiConfigure;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiHeartbeat;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiLeave;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiNak;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiRedirect;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiUnassign;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.net.ConnectionLostException;
import io.stat.nabuproject.core.net.NodeLeavingException;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.leader.ElectedLeaderProvider;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionListener;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
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
class ConnectionImpl implements NabuConnection {
    private static final AttributeKey<Long> LAST_SEQUENCE = AttributeKey.newInstance("last_heartbeat");

    private final NabuConnectionListener connectionListener;
    private final ThrottlePolicyProvider throttlePolicyProvider;
    private final KafkaBrokerConfigProvider kafkaBrokerConfigProvider;
    private final ElectedLeaderProvider electedLeaderProvider;

    private final ChannelHandlerContext context;
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

    public ConnectionImpl(ChannelHandlerContext context,
                          NabuConnectionListener connectionListener,
                          ThrottlePolicyProvider throttlePolicyProvider,
                          KafkaBrokerConfigProvider kafkaBrokerConfigProvider,
                          ElectedLeaderProvider electedLeaderProvider) {

        this.context = context;
        this.connectionListener = connectionListener;
        this.throttlePolicyProvider = throttlePolicyProvider;
        this.kafkaBrokerConfigProvider = kafkaBrokerConfigProvider;
        this.electedLeaderProvider = electedLeaderProvider;

        this.context.attr(LAST_SEQUENCE).set(0L);

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

        this.heartbeatTimer = new Timer(String.format("NabuConnection-Heartbeat-%s", prettyName()));
        this.leaveEnforcerTimer = new Timer(String.format("NabuConnection-LeaveEnforcer-%s", prettyName()));

        this.heartbeatTask = this.new HeartbeatTask();
        this.leaveEnforcerTask = this.new LeaveEnforcerTask();

        logger.info("New ConnectionImpl({}) for {}", this, context);

        if(this.electedLeaderProvider.isSelf()) {
            logger.info("This node is the leader; sending CONFIGURE");
            dispatchConfigure(buildDispatchedNabuConfig()).thenAcceptAsync(this::confirmConnection);
            // todo: heartbeat timeouts should be configurable.
            // 1 second delay before first run, 3 second delay between runs
            this.heartbeatTimer.scheduleAtFixedRate(this.heartbeatTask, 1000, 3000);

            connectionListener.onNewNabuConnection(this);
        } else {
            logger.info("This node IS NOT the leader; sending REDIRECT");
            // have to free up the event loop thread before I can dispatch the redirect.
            // im reusing the hearbeat timer since basically else will use it.
            this.heartbeatTimer.schedule(new RedirectorTask(), 1000);
        }
    }

    private Map<String, Serializable> buildDispatchedNabuConfig() {
        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.builder();

        ImmutableList<String> kafkaBrokers = ImmutableList.copyOf(kafkaBrokerConfigProvider.getKafkaBrokers());
        String kafkaGroup                  = kafkaBrokerConfigProvider.getKafkaGroup();

        ImmutableList<ThrottlePolicy> tps  = ImmutableList.copyOf(throttlePolicyProvider.getThrottlePolicies());

        builder.put(EnkiSourcedConfigKeys.KAFKA_BROKERS, kafkaBrokers)
               .put(EnkiSourcedConfigKeys.KAFKA_GROUP, kafkaGroup)
               .put(EnkiSourcedConfigKeys.THROTTLE_POLICIES, tps);

        return builder.build();
    }

    private CompletableFuture<EnkiPacket> dispatchHeartbeat() {
        return dispatchPacket(new EnkiHeartbeat(assignSequence()));
    }
    private CompletableFuture<EnkiPacket> dispatchKick() { return dispatchPacket(new EnkiLeave(assignSequence())); }
    private CompletableFuture<EnkiPacket> dispatchConfigure(Map<String, Serializable> configPacketData) {
        return dispatchPacket(new EnkiConfigure(assignSequence(), configPacketData));
    }

    private void confirmConnection(EnkiPacket ep) {
        logger.info("confirmCnxn {}", ep);
        if(ep.getType() == EnkiPacket.Type.ACK) {
            connectionListener.onNabuReady(this);
        } else {
            killConnection();
        }
    }

    private void performRedirect(AddressPort ap) {
        dispatchPacket(new EnkiRedirect(assignSequence(), ap.getAddress(), ap.getPort()))
            .thenAcceptAsync($$$$$$ -> {
                logger.info("acceptored");
                killConnection(true);
            });
    }

    private void dispatchAck(long sequence) { dispatchAck(sequence, false); }
    private void dispatchAck(long sequence, boolean ignoreDC) {
        dispatchEphemeral(new EnkiAck(sequence), ignoreDC);
    }

    private void dispatchNak(long sequence) { dispatchNak(sequence, false); }
    private void dispatchNak(long sequence, boolean ignoreDC) {
        dispatchEphemeral(new EnkiNak(sequence), ignoreDC);
    }

    // ephemeral because it has no future waiting behind it ?
    // or something, i guess :/
    private void dispatchEphemeral(EnkiPacket packet, boolean ignoreDisconnect) {
        if(!isDisconnecting.get() || ignoreDisconnect) {
            context.writeAndFlush(packet);
        }
    }

    private CompletableFuture<EnkiPacket> dispatchPacket(EnkiPacket packet) { return dispatchPacket(packet, false); }
    private CompletableFuture<EnkiPacket> dispatchPacket(EnkiPacket packet, boolean ignoreDisconnect) {
        CompletableFuture<EnkiPacket> future = new CompletableFuture<>();
        connectionListener.onPacketDispatched(this, packet, future);
        if (!isDisconnecting.get()) {
            promises.put(packet.getSequenceNumber(), future);
            context.writeAndFlush(packet);
        } else if (packet.getType() == EnkiPacket.Type.LEAVE || ignoreDisconnect) {
            promises.put(packet.getSequenceNumber(), future);
            context.writeAndFlush(packet);
        } else {
            future.completeExceptionally(
                    new NodeLeavingException(
                        "Cannot dispatch the packet as" +
                        " this node is disconnecting from the cluster"));
        }

        if (packet.getType() != EnkiPacket.Type.ACK) {
            connectionListener.onPacketDispatched(this, packet, future);
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
    public void leaveGracefully() {
        logger.info("leaveGracefully called on {}", this);

        nabuLeaving(true);

        // todo: leave enforcer timeout should be configurable.
        leaveEnforcerTimer.schedule(leaveEnforcerTask, 5000);

        dispatchKick().whenCompleteAsync((packet, exception) -> {
            if(exception != null && !(exception instanceof ConnectionLostException)) {
                logger.error("Dispatch leaveGracefully promise fulfilled with unexpected exception: ", exception);
            } else if (exception != null && exception instanceof ConnectionLostException) {
                logger.info("The connection to Nabu was terminated while waiting for a response to a LEAVE.");
            } else {
                this.leaveAcknowledged.set(true);
                killConnection(true);
            }
        });
    }

    @Override
    public void killConnection() {
        killConnection(false);
    }

    private void killConnection(boolean suppressWarning) {
        if(!suppressWarning) {
            logger.warn("killConnection() called. This is going to be ugly.");
        }

        context.close();
    }

    /**
     * common functionality for when leaveGracefully() is called vs. client notifies that
     * it's leaving.
     * @param serverInitiated true for leaveGracefully(), false for client-initiated
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
                                    "and will not be able to respond to this request. (sequence " + seq + ")")));
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
                            "has been lost (sequence " + seq + ")")));
        }

        heartbeatTimer.cancel();
        leaveEnforcerTimer.cancel();

        EnkiConnection.DisconnectCause cause;
        if(wasLeavePacketSent.get()) {
            if(wasLeaveServerInitiated.get()) {
                cause = EnkiConnection.DisconnectCause.SERVER_LEAVE_REQUEST;
            } else {
                cause = EnkiConnection.DisconnectCause.CLIENT_LEAVE_REQUEST;
            }
        } else {
            cause = EnkiConnection.DisconnectCause.CONNECTION_RESET;
        }
        connectionListener.onNabuDisconnected(this, cause, leaveAcknowledged.get());
    }

    @Override @Synchronized
    public void onPacketReceived(EnkiPacket packet) {
        connectionListener.onPacketReceived(this, packet);
        long sequence = packet.getSequenceNumber();
        CompletableFuture<EnkiPacket> f = promises.getOrDefault(sequence, null);
        if(f != null) {
            promises.remove(sequence);
            f.complete(packet);
        } else {
            if (sequence == lastIncomingSequence.get() + 1L) {
                logger.info("Wow! received a fantastic packet: {}", packet);

                if(packet.getType() == EnkiPacket.Type.LEAVE) {
                    nabuLeaving(false);
                    dispatchAck(packet.getSequenceNumber());
                } else {
                    if(packet.getType() != EnkiPacket.Type.ACK && packet.getType() != EnkiPacket.Type.NAK) {
                        // todo: client shouldn't be sending anything else. what to do?
                        logger.error("RECEIVED A PACKET THAT CLIENTS SHOULDNT BE SENDING :: {}", packet);
                        // todo: dispatch that motherfucker.
                    }
                }
            } else {
                logger.error("RECEIVED AN UNEXPECTED PACKET (sequence out of order) :: {}", packet);
                // todo: leaveGracefully the client?
            }
        }
    }

    @Override
    public CompletableFuture<EnkiPacket> sendAssign(TopicPartition assignment) {
        return dispatchPacket(new EnkiAssign(assignSequence(), assignment.topic(), assignment.partition()));
    }

    @Override
    public CompletableFuture<EnkiPacket> sendUnassign(TopicPartition assignment) {
        return dispatchPacket(new EnkiUnassign(assignSequence(), assignment.topic(), assignment.partition()));
    }

    @Override @Synchronized
    public String toString() {
        MoreObjects.ToStringHelper tsh = MoreObjects.toStringHelper(this);
        tsh.add("addr", prettyName())
           .add("isDisconnecting", isDisconnecting.get())
           .add("lastOutgoingSequence", lastOutgoingSequence.get());

        if(waitingForHeartbeat.get()) {
            tsh.add("waitingForHeartbeat", true);
            tsh.add("missedHeartbeats", missedHeartbeats.get());
        }

        tsh.add("outstandingPromises", promises.size());

        return tsh.toString();
    }

    @Override
    public String prettyName() {
        InetSocketAddress remoteAddress = ((InetSocketAddress) context.channel().remoteAddress());
        return remoteAddress.getAddress() + ":" + remoteAddress.getPort();
    }

    @Override
    public void nak(EnkiPacket packet) {
        dispatchNak(packet.getSequenceNumber());
    }

    @Override
    public void ack(EnkiPacket packet) {
        dispatchAck(packet.getSequenceNumber());
    }

    /**
     * A task that sends out heartbeats, and boots the client if too many are missed.
     */
    private class HeartbeatTask extends TimerTask {
        @Override
        public void run() {
            if(waitingForHeartbeat.get()) {
                int missedHeartbeats = ConnectionImpl.this.missedHeartbeats.incrementAndGet();
                logger.warn("{} missed heartbeat(s) since last heartbeat...", missedHeartbeats);

                // todo: this should be configurable.
                if(missedHeartbeats >= 5) {
                    logger.error("Nabu node has missed {} heartbeats now!", missedHeartbeats);
                    // todo: force deallocate and leaveGracefully from cluster.
                    ConnectionImpl.this.leaveGracefully();
                }
            } else {
                waitingForHeartbeat.set(true);
                CompletableFuture<EnkiPacket> f = ConnectionImpl.this.dispatchHeartbeat();
                f.whenCompleteAsync((enkiPacket, throwable) -> {
                    if(throwable == null) {
                        ConnectionImpl.this.lastHeartbeat.set(System.currentTimeMillis());
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
            if(!ConnectionImpl.this.leaveAcknowledged.get()) {
                logger.error("Server-initiated leave request was NOT acknowledged..");
                logger.error("Forcibly closing the connection to {}. " +
                        "You may want to leaveGracefully the node to ensure data doesn't get re-written!",
                        ConnectionImpl.this);
            } else {
                logger.debug("No need to enforce leave request for {}", ConnectionImpl.this);
            }
        }
    }

    private class RedirectorTask extends TimerTask {
        @Override
        public void run() {
            AddressPort electedAP = electedLeaderProvider.getElectedLeaderData().getAddressPort();
            performRedirect(electedAP);
        }
    }
}
