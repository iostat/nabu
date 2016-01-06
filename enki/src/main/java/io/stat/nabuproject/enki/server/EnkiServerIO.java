package io.stat.nabuproject.enki.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.util.AttributeKey;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.leader.ElectedLeaderProvider;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionListener;
import lombok.extern.slf4j.Slf4j;

/**
 * Handles Enki server-side IO.
 *
 * This really needs to be package-protected, but then FluentChannelInitializer
 * can't instantiate it.
 */
@Slf4j
public final class EnkiServerIO extends SimpleChannelInboundHandler<EnkiPacket> {
    private static final AttributeKey<NabuConnection> CONNECTED_NABU_ATTR = AttributeKey.newInstance("connected_nabu");
    private final NabuConnectionListener toNotify;
    private final ThrottlePolicyProvider throttlePolicyProvider;
    private final KafkaBrokerConfigProvider kafkaBrokerConfigProvider;
    private final ChannelGroup channelGroup;
    private final ElectedLeaderProvider electedLeaderProvider;

    public EnkiServerIO(NabuConnectionListener toNotify,
                        ChannelGroup channelGroup,
                        ThrottlePolicyProvider throttlePolicyProvider,
                        KafkaBrokerConfigProvider kafkaBrokerConfigProvider,
                        ElectedLeaderProvider electedLeaderProvider) {
        super();
        this.channelGroup = channelGroup;
        this.throttlePolicyProvider = throttlePolicyProvider;
        this.kafkaBrokerConfigProvider = kafkaBrokerConfigProvider;
        this.toNotify = toNotify;
        this.electedLeaderProvider = electedLeaderProvider;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("CHANNEL_ACTIVE: {}", ctx);
        channelGroup.add(ctx.channel());
        ctx.attr(CONNECTED_NABU_ATTR).set(
                new ConnectionImpl(
                        ctx,
                        toNotify,
                        throttlePolicyProvider,
                        kafkaBrokerConfigProvider,
                        electedLeaderProvider
        ));
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.error("CHANNEL_INACTIVE");
        getNabu(ctx).onDisconnected();
        ctx.attr(CONNECTED_NABU_ATTR).getAndRemove();
        channelGroup.remove(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // todo: no comment....
        logger.error("EXCEPTION_CAUGHT: {}", cause);
        getNabu(ctx).leaveGracefully();
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
