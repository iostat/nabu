package io.stat.nabuproject.core.enkiprotocol;

import com.google.inject.Inject;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAck;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacketType;
import lombok.extern.slf4j.Slf4j;

/**
 * A client for the Enki protocol, used by Nabu.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class EnkiClient extends Component {

    private EnkiAddressProvider config;
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;
    private Channel clientChannel;

    @Inject
    public EnkiClient(EnkiAddressProvider provider) {
        this.config = provider;
        this.eventLoopGroup = new NioEventLoopGroup();
    }

    @Override
    public void start() throws ComponentException {
        if(!config.isEnkiDiscovered()) {
            throw new ComponentException(true, "EnkiAddressProvider did not discover any Enkis!");
        }

        this.bootstrap = new Bootstrap();
        this.bootstrap.group(eventLoopGroup)
                      .channel(NioSocketChannel.class)
                      .option(ChannelOption.TCP_NODELAY, true) // the enki protocol is really tiny. john nagle is not our friend.
                      .handler(new EnkiChannelInitializer(EnkiClientHandler.class));

        try {
            this.clientChannel = this.bootstrap
                                     .connect(config.getEnkiHost(), config.getEnkiPort())
                                     .sync()
                                     .channel();
        } catch (InterruptedException e) {
            this.eventLoopGroup.shutdownGracefully();

            logger.error("Failed to start EnkiClient, {}", e);
            throw new ComponentException(true, e);
        }
    }

    @Override
    public void shutdown() throws ComponentException {
        this.clientChannel.close();
        this.eventLoopGroup.shutdownGracefully();
    }

    /**
     * The name is a bit misleading, as this <i>technically</i> handles a server (meaning, it
     * responds to packets sent BY the server.)
     */
    public static class EnkiClientHandler extends SimpleChannelInboundHandler<EnkiPacket> {
        public EnkiClientHandler() { super(); }
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, EnkiPacket msg) throws Exception {
            logger.info("channelRead0: {}", msg);
            if(msg.getType() == EnkiPacketType.HEARTBEAT) {
                logger.info("heartbeat!!!");
                ctx.writeAndFlush(new EnkiAck(msg.getSequenceNumber()));
            }
        }
    }
}
