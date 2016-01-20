package io.stat.nabuproject.nabu.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.stat.nabuproject.nabu.common.command.IdentifyCommand;
import io.stat.nabuproject.nabu.common.command.NabuCommand;
import io.stat.nabuproject.nabu.common.response.IDResponse;
import io.stat.nabuproject.nabu.common.response.NabuResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Handles Nabu client communication.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
final class NabuClientIO extends SimpleChannelInboundHandler<NabuResponse> {
    private final HighLevelNabuClientBridge bridge;
    private final AtomicReference<String> expectedClusterName;

    private final AtomicLong lastIncomingSequence;
    private final AtomicLong lastOutgoingSequence;
    private final AtomicLong identifySequence;

    private final AtomicBoolean identifyDispatched;
    private final AtomicBoolean identifyReceived;

    private final AtomicReference<ChannelHandlerContext> context;
    private final byte[] $lock;


    NabuClientIO(HighLevelNabuClientBridge bridge, String expectedClusterName) {
        this.bridge = bridge;
        this.expectedClusterName = new AtomicReference<>(expectedClusterName);

        this.lastIncomingSequence = new AtomicLong(0);
        this.lastOutgoingSequence = new AtomicLong(0);
        this.identifySequence = new AtomicLong(Long.MIN_VALUE); // basically impossible!
        this.identifyDispatched = new AtomicBoolean(false);
        this.identifyReceived = new AtomicBoolean(false);

        this.$lock = new byte[0];

        this.context = new AtomicReference<>(null);

        this.bridge.setClientIO(this);
        logger.debug("Constructed a new NabuClientIO!");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Caught an unexpected exception", cause);
        // todo: bubble failure to bridge.
        bridge.connectionInterrupted(this, cause);
        ctx.channel().close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        context.set(ctx);
        logger.info("Nabu client connection established, sending IDENTIFY.");

        long assignedSequence = assignNextSequence();
        synchronized ($lock) {
            identifySequence.set(assignedSequence);
            identifyDispatched.set(true);
            ctx.writeAndFlush(new IdentifyCommand(assignedSequence));
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Nabu client connection closed.");
        bridge.connectionLost(this);
        ctx.channel().close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NabuResponse msg) throws Exception {
        logger.debug("Received a NabuResponse! :: {}", msg); // todo: change to debug.
        if(msg instanceof IDResponse) {
            if(!identifyReceived.get()) {
                String remoteClusterName = ((IDResponse)msg).getData();
                String expectedClusterName = this.expectedClusterName.get();
                if(msg.getSequence() == identifySequence.get()) {
                    identifyReceived.set(true);
                    if(remoteClusterName.equals(expectedClusterName)) {
                        bridge.connectionEstablished(this);
                    } else {
                        bridge.identificationFailed(this, expectedClusterName, remoteClusterName);
                    }
                } else {
                    logger.error("Only one IDENTIFY is ever sent, and we received the wrong sequence number back.");
                }
            } else {
                logger.error("Only one IDENTIFY is ever sent, and we already received one before.");
            }
        } else {
            bridge.responseReceived(this, msg);
        }

    }

    void dispatchCommand(NabuCommand cmd) {
        context.get().writeAndFlush(cmd);
    }

    long assignNextSequence() {
        return lastOutgoingSequence.getAndIncrement();
    }
}
