package com.socialrank.nabu.server;

import com.socialrank.nabu.protocol.NabuCommandDecoder;
import com.socialrank.nabu.protocol.NabuResponseEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class NabuChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline cp = ch.pipeline();

        cp.addLast(
            new NabuResponseEncoder(),
            new NabuCommandDecoder(),
            new NabuCommandInboundHandler()
        );
    }
}
