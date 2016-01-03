package io.stat.nabuproject.core.enkiprotocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventListener;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import lombok.extern.slf4j.Slf4j;

/**
 * The name is a bit misleading, as this <i>technically</i> handles a server (meaning, it
 * responds to packets sent BY the server.)
 */
@Slf4j
public class EnkiClientIO extends SimpleChannelInboundHandler<EnkiPacket> {
    private static final AttributeKey<EnkiConnectionImpl> CONNECTED_ENKI_ATTR = AttributeKey.valueOf("connected_enki");
    private final EnkiClientEventListener toNotify;

    public EnkiClientIO(EnkiClientEventListener toNotify) {
        super();
        this.toNotify = toNotify;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("CHANNEL_ACTIVE: {}", ctx);
        ctx.attr(CONNECTED_ENKI_ATTR).set(
                new EnkiConnectionImpl(ctx, toNotify));
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.error("CHANNEL_INACTIVE");
        getEnki(ctx).onDisconnected();
        ctx.attr(CONNECTED_ENKI_ATTR).getAndRemove();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // todo: no comment....
        logger.error("EXCEPTION_CAUGHT: {}", cause);
        getEnki(ctx).leave();
        super.exceptionCaught(ctx, cause);
    }

    private EnkiConnectionImpl getEnki(ChannelHandlerContext ctx) {
        return ctx.attr(CONNECTED_ENKI_ATTR).get();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, EnkiPacket msg) throws Exception {
        logger.trace("channelRead0: {}", msg);
        getEnki(ctx).onPacketReceived(msg);
    }
}
