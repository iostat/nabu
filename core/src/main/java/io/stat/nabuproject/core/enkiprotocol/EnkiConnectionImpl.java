package io.stat.nabuproject.core.enkiprotocol;

import io.netty.channel.ChannelHandlerContext;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAck;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAssign;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiConfigure;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiHeartbeat;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiLeave;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiNak;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by io on 1/2/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class EnkiConnectionImpl implements EnkiConnection {
    private final ChannelHandlerContext ctx;
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

    public EnkiConnectionImpl(ChannelHandlerContext ctx,
                              EnkiClientEventListener toNotify) {
        this.ctx = ctx;
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
    }


    @Synchronized @Override
    public void leave() {
        isDisconnecting.set(true);
        EnkiLeave leave = new EnkiLeave(assignSequence());
        dispatchPacket(leave);
        sequenceNumberOfLeave.set(leave.getSequenceNumber());
        wasLeavePacketSent.set(true);
        wasLeaveServerInitiated.set(false);
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
        toNotify.onConnectionLost(this, wasLeavePacketSent.get(), wasLeaveServerInitiated.get(), wasLeaveAcknowledged.get());
    }

    public void onPacketReceived(EnkiPacket p) {
        logger.info("onPacketReceived: {}", p);
        lastIncomingSequence.set(p.getSequenceNumber());
        switch(p.getType()) {
            case HEARTBEAT:
                lastHeartbeatTimestamp.set(((EnkiHeartbeat) p).getTimestamp());
                dispatchPacket(new EnkiAck(p.getSequenceNumber()));
                break;
            case CONFIGURE:
                lastConfigTimestamp.set(System.currentTimeMillis());
                toNotify.onConfigurationReceived(this, ((EnkiConfigure)p).getOptions());
                break;
            case LEAVE:
                // todo: LEAVE
                wasLeaveServerInitiated.set(true);
                wasLeavePacketSent.set(true);
                dispatchPacket(new EnkiAck(p.getSequenceNumber()));
                wasLeaveAcknowledged.set(true);
                break;
            case ACK:
                if(p.getSequenceNumber() == sequenceNumberOfLeave.get()) {
                    wasLeaveAcknowledged.set(true);
                }
                break;
            case ASSIGN:
            case UNASSIGN:
                EnkiAssign packet = ((EnkiAssign) p);
                TopicPartition tp = new TopicPartition(packet.getIndexName(), packet.getPartitionNumber());
                boolean isAssign = (p.getType() == EnkiPacket.Type.ASSIGN);

                if(isAssign) {
                    toNotify.onTaskAssigned(this, tp);
                } else {
                    toNotify.onTaskUnassigned(this, tp);
                }
                break;
        }
    }

    private void dispatchPacket(EnkiPacket p) {
        ctx.writeAndFlush(p);
    }

    private long assignSequence() {
        return lastOutgoingSequence.incrementAndGet();
    }
}
