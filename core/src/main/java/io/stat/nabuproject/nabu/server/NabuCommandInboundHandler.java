package io.stat.nabuproject.nabu.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.stat.nabuproject.common.NabuCommand;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class NabuCommandInboundHandler extends SimpleChannelInboundHandler<NabuCommand> {

    public NabuCommandInboundHandler() {
        //this.commandRouter = Nabu.g
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NabuCommand msg) throws Exception {

    }
}
