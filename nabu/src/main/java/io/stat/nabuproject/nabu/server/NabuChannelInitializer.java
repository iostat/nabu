package io.stat.nabuproject.nabu.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.stat.nabuproject.nabu.protocol.CommandDecoder;
import io.stat.nabuproject.nabu.protocol.ResponseEncoder;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class NabuChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline cp = ch.pipeline();

        cp.addLast(
            new ResponseEncoder(),
            new CommandDecoder(),
            new NabuCommandInboundHandler()
        );
    }
}
