package com.socialrank.nabu.bootstrap;

import com.socialrank.nabu.server.NabuChannelInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import static com.socialrank.nabu.config.NabuConfig.getNabuConfig;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
@Slf4j
public final class NabuServerBootstrap {
    private final ServerBootstrap bootstrap;

    public NabuServerBootstrap() {
        this.bootstrap = new ServerBootstrap();
    }

    public void bootstrapAndStart() throws InterruptedException {
        int acceptorThreads = getNabuConfig().getAcceptorThreads();
        int workerThreads   = getNabuConfig().getWorkerThreads();

        String bindAddress = getNabuConfig().getListenAddress();
        int bindPort = getNabuConfig().getListenPort();

        @Cleanup("shutdownGracefully") EventLoopGroup acceptorGroup = new NioEventLoopGroup(acceptorThreads);
        @Cleanup("shutdownGracefully") EventLoopGroup workerGroup = new NioEventLoopGroup(workerThreads);

        bootstrap.group(acceptorGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .handler(new LoggingHandler())
         .childHandler(new NabuChannelInitializer());

        logger.info("Binding Nabu on {}:{}, with {} acceptor thread(s) and {} worker thread(s)",
                bindAddress, bindPort, acceptorThreads, workerThreads);

        bootstrap.bind(bindAddress, bindPort).sync().channel().closeFuture().sync();
    }
}
