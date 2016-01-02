package io.stat.nabuproject.enki.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.AttributeKey;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import lombok.extern.slf4j.Slf4j;

/**
 * Handles Enki server-side IO.
 */
@Slf4j
public class EnkiServerIO extends SimpleChannelInboundHandler<EnkiPacket> {
    private static final AttributeKey<NabuConnection> CONNECTED_NABU_ATTR = AttributeKey.newInstance("connected_nabu");
    private final NabuConnectionListener toNotify;

    public EnkiServerIO(NabuConnectionListener toNotify) {
        super();
        this.toNotify = toNotify;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("CHANNEL_ACTIVE: {}", ctx);
        ctx.attr(CONNECTED_NABU_ATTR).set(new NabuConnectionImpl(ctx, toNotify));
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.error("CHANNEL_INACTIVE");
        getNabu(ctx).onDisconnected();
        ctx.attr(CONNECTED_NABU_ATTR).getAndRemove();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // todo: no comment....
        logger.error("EXCEPTION_CAUGHT: {}", cause);
        getNabu(ctx).kick();
        super.exceptionCaught(ctx, cause);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, EnkiPacket msg) throws Exception {
        logger.debug("channelRead0: {}", msg);
        getNabu(ctx).onPacketReceived(msg);
    }

    private static NabuConnection getNabu(ChannelHandlerContext ctx) {
        return ctx.attr(CONNECTED_NABU_ATTR).get();
    }
}
