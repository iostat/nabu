package io.stat.nabuproject.nabu.server;

import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.net.FluentChannelInitializer;
import io.stat.nabuproject.nabu.NabuConfig;
import io.stat.nabuproject.nabu.protocol.CommandDecoder;
import io.stat.nabuproject.nabu.protocol.ResponseEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * Runs the Nabu Netty Server.
 */
@Slf4j
class ServerImpl extends NabuServer {
    private NabuConfig config;

    private ServerBootstrap bootstrap;
    private Channel listenerChannel;

    private int acceptorThreads;
    private int workerThreads;
    private String bindAddress;
    private int bindPort;

    private EventLoopGroup acceptorGroup;
    private EventLoopGroup workerGroup;

    private final FluentChannelInitializer channelInitializer;

    @Inject
    ServerImpl(NabuConfig config) {
        this.config = config;

        this.bootstrap = new ServerBootstrap();

        this.acceptorThreads = this.config.getAcceptorThreads();
        this.workerThreads   = this.config.getWorkerThreads();
        this.bindAddress     = this.config.getListenAddress();
        this.bindPort        = this.config.getListenPort();

        this.acceptorGroup = new NioEventLoopGroup(acceptorThreads);
        this.workerGroup   = new NioEventLoopGroup(workerThreads);

        this.channelInitializer =
                new FluentChannelInitializer()
                        .addHandler(ResponseEncoder.class)
                        .addHandler(CommandDecoder.class)
                        .addHandler(NabuCommandInboundHandler.class);
    }

    @Override
    public void start() throws ComponentException {
        bootstrap.group(acceptorGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .handler(new LoggingHandler())
         .childHandler(channelInitializer);

        logger.info("Binding NettyServer on {}:{}, with {} acceptor thread(s) and {} worker thread(s)",
                bindAddress, bindPort, acceptorThreads, workerThreads);

        try {
            this.listenerChannel = bootstrap.bind(bindAddress, bindPort).sync().channel();
        } catch(InterruptedException e) {
            this.acceptorGroup.shutdownGracefully();
            this.workerGroup.shutdownGracefully();

            logger.error("Failed to start Nabu server, {}", e);
            throw new ComponentException(true, e);
        }
    }

    @Override
    public void shutdown() throws ComponentException {
        // TODO: fixes for shutdown() called on failed initialization (not critical)
        logger.info("Shutting down NabuServer (over Netty)...");
        this.listenerChannel.close();

        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
