package com.socialrank.nabu.server;

import com.socialrank.nabu.common.NabuCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class NabuCommandInboundHandler extends SimpleChannelInboundHandler<NabuCommand> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NabuCommand msg) throws Exception {

    }
}
