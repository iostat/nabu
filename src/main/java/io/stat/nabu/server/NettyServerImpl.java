package io.stat.nabu.server;

import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.stat.nabu.config.ConfigurationProvider;
import io.stat.nabu.core.ComponentException;
import lombok.extern.slf4j.Slf4j;

/**
 * Runs the Nabu Netty Server.
 */
@Slf4j
public class NettyServerImpl extends NabuServer {
    private ConfigurationProvider config;

    private ServerBootstrap bootstrap;
    private Channel listenerChannel;

    private int acceptorThreads;
    private int workerThreads;
    private String bindAddress;
    private int bindPort;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    @Inject
    NettyServerImpl(ConfigurationProvider config) {
        this.config = config;

        this.bootstrap = new ServerBootstrap();

        this.acceptorThreads = config.getAcceptorThreads();
        this.workerThreads   = config.getWorkerThreads();
        this.bindAddress     = config.getListenAddress();
        this.bindPort        = config.getListenPort();

        this.bossGroup   = new NioEventLoopGroup(acceptorThreads);
        this.workerGroup = new NioEventLoopGroup(workerThreads);
    }

    @Override
    public void start() throws ComponentException {
        bootstrap.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .handler(new LoggingHandler())
         .childHandler(new NabuChannelInitializer());

        logger.info("Binding NettyServer on {}:{}, with {} acceptor thread(s) and {} worker thread(s)",
                bindAddress, bindPort, acceptorThreads, workerThreads);

        try {
            this.listenerChannel = bootstrap.bind(bindAddress, bindPort).sync().channel();
        } catch(InterruptedException e) {
            this.bossGroup.shutdownGracefully();
            this.workerGroup.shutdownGracefully();

            logger.error("Failed to start NettyServer, {}", e);
            throw new ComponentException(true, e);
        }
    }

    @Override
    public void shutdown() throws ComponentException {
        logger.info("Shutting down NettyServer...");
        this.listenerChannel.close();

        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
