package io.stat.nabuproject.enki.server;

import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.net.FluentChannelInitializer;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.EnkiConfig;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionEventSource;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionListener;
import io.stat.nabuproject.enki.server.dispatch.NabuConnectionListenerDispatcher;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

/**
 * Runs the Enki management server.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat)
 */
@Slf4j
class ServerImpl extends EnkiServer {
    private ServerBootstrap bootstrap;
    private Channel listenerChannel;

    private int acceptorThreads;
    private int workerThreads;
    private String bindAddress;
    private int bindPort;

    private EventLoopGroup acceptorGroup;
    private EventLoopGroup workerGroup;

    @Delegate(types=NabuConnectionEventSource.class)
    private final NabuConnectionListenerDispatcher dispatcher;

    private final FluentChannelInitializer channelInitializer;

    @Inject
    ServerImpl(EnkiConfig config) {
        this.bootstrap = new ServerBootstrap();

        this.acceptorThreads = config.getAcceptorThreads();
        this.workerThreads   = config.getWorkerThreads();
        this.bindAddress     = config.getListenAddress();
        this.bindPort        = config.getListenPort();

        this.acceptorGroup = new NioEventLoopGroup(acceptorThreads);
        this.workerGroup = new NioEventLoopGroup(workerThreads);

        this.dispatcher = new NabuConnectionListenerDispatcher();

        this.channelInitializer = new FluentChannelInitializer();
        channelInitializer.addHandler(EnkiPacket.Encoder.class);
        channelInitializer.addHandler(EnkiPacket.Decoder.class);
        channelInitializer.addHandler(EnkiServerIO.class,
                new Object[]{
                        dispatcher,
                        config,
                        config
                },
                new Class[]{
                        NabuConnectionListener.class,
                        ThrottlePolicyProvider.class,
                        KafkaBrokerConfigProvider.class
                });

    }

    @Override
    public void start() throws ComponentException {
        bootstrap.group(acceptorGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true) // the enki protocol is really tiny. john nagle is not our friend.
                .handler(new LoggingHandler())
                .childHandler(channelInitializer);

        logger.info("Binding NettyServer on {}:{}, with {} acceptor thread(s) and {} worker thread(s)",
                bindAddress, bindPort, acceptorThreads, workerThreads);

        try {
            this.listenerChannel = bootstrap.bind(bindAddress, bindPort).sync().channel();
        } catch(InterruptedException e) {
            this.acceptorGroup.shutdownGracefully();
            this.workerGroup.shutdownGracefully();

            logger.error("Failed to start Enki Server, {}", e);
            throw new ComponentException(true, e);
        }
    }

    @Override
    public void shutdown() throws ComponentException {
        // todo: disconnect all connected nodes. (or will the worker-coordinator or whatever do that first?)
        // todo: probably shut down the dispatcher too :/
        // TODO: fixes for shutdown() called on failed initialization (not critical)
        logger.info("Shutting down Enki server...");
        this.listenerChannel.close();

        // todo: close all dispatched listeners before actually shutting everything down...
        dispatcher.shutdown();

        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

}
