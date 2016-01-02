package io.stat.nabuproject.core.enkiprotocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAck;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import lombok.extern.slf4j.Slf4j;

/**
 * The name is a bit misleading, as this <i>technically</i> handles a server (meaning, it
 * responds to packets sent BY the server.)
 */
@Slf4j
public class EnkiClientIO extends SimpleChannelInboundHandler<EnkiPacket> {
    public EnkiClientIO() { super(); }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, EnkiPacket msg) throws Exception {
        logger.trace("channelRead0: {}", msg);
        if(msg.getType() == EnkiPacket.Type.HEARTBEAT) {
            logger.info("heartbeat!!!");
            ctx.writeAndFlush(new EnkiAck(msg.getSequenceNumber()));
        }
    }
}
