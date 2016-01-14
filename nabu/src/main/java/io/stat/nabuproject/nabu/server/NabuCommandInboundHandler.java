package io.stat.nabuproject.nabu.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.stat.nabuproject.core.elasticsearch.ESConfigProvider;
import io.stat.nabuproject.nabu.common.command.IdentifyCommand;
import io.stat.nabuproject.nabu.common.command.NabuCommand;
import io.stat.nabuproject.nabu.common.response.NabuResponse;
import io.stat.nabuproject.nabu.router.CommandRouter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Performs low-level server-side IO with Nabu clients.
 *
 * Needs to be public so that FluentChannelInitializer can initialize it.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class NabuCommandInboundHandler extends SimpleChannelInboundHandler<NabuCommand> implements NabuCommandSource {
    private final ESConfigProvider esc;
    private final CommandRouter cr;
    private final AtomicReference<ChannelHandlerContext> context;

    public NabuCommandInboundHandler(@NonNull ESConfigProvider esc,
                                     @NonNull CommandRouter cr) {
        this.esc = esc;
        this.cr = cr;
        this.context = new AtomicReference<>(null);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NabuCommand msg) throws Exception {
        if(msg instanceof IdentifyCommand) {
            String escName = esc.getESClusterName();
            ctx.writeAndFlush(((IdentifyCommand)msg).identifyResponse(escName));
            logger.info("Received an IdentifyCommand from {}, responding with {}", ctx.channel().remoteAddress(), escName);
        } else {
            cr.inboundCommand(this, msg);
        }
    }

    @Override
    public void respond(NabuResponse toSend) {
        context.get().writeAndFlush(toSend);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.context.set(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }
}
