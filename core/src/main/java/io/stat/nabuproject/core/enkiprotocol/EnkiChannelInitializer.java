package io.stat.nabuproject.core.enkiprotocol;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;

/**
 * A Netty ChannelInitializer that speaks in EnkiPackets
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final ChannelHandler[] allHandlers;

    public EnkiChannelInitializer() {
        this(new ChannelHandler[0]);
    }
    public EnkiChannelInitializer(ChannelHandler... extraHandlers) {
        allHandlers = new ChannelHandler[extraHandlers.length + 2];

        allHandlers[0] = new EnkiPacket.Encoder();
        allHandlers[1] = new EnkiPacket.Decoder();
        System.arraycopy(extraHandlers, 0, allHandlers, 2, extraHandlers.length);
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline cp = ch.pipeline();

        cp.addLast(allHandlers);
    }
}
