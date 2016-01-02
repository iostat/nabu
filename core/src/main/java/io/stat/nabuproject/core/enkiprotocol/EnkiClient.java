package io.stat.nabuproject.core.enkiprotocol;

import com.google.inject.Inject;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.net.FluentChannelInitializer;
import lombok.extern.slf4j.Slf4j;

/**
 * A client for the Enki protocol, used by Nabu.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class EnkiClient extends Component {

    private final FluentChannelInitializer channelInitializer;
    private EnkiAddressProvider config;
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;
    private Channel clientChannel;

    @Inject
    public EnkiClient(EnkiAddressProvider provider) {
        this.config = provider;
        this.eventLoopGroup = new NioEventLoopGroup();
        this.channelInitializer = new FluentChannelInitializer();
        this.channelInitializer.addHandler(EnkiPacket.Encoder.class);
        this.channelInitializer.addHandler(EnkiPacket.Decoder.class);
        this.channelInitializer.addHandler(EnkiClientIO.class);
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
                      .handler(channelInitializer);

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
}
