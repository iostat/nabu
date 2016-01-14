package io.stat.nabuproject.nabu.server;

import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESConfigProvider;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.net.NetworkServerConfigProvider;
import io.stat.nabuproject.core.net.channel.FluentChannelInitializer;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import io.stat.nabuproject.nabu.protocol.CommandDecoder;
import io.stat.nabuproject.nabu.protocol.ResponseEncoder;
import io.stat.nabuproject.nabu.router.CommandRouter;
import lombok.extern.slf4j.Slf4j;

/**
 * Runs the Nabu Netty Server.
 */
@Slf4j
class ServerImpl extends NabuServer {
    private final ESConfigProvider esConfigProvider;
    private final CommandRouter commandRouter;
    private ServerBootstrap bootstrap;
    private Channel listenerChannel;

    private final int acceptorThreads;
    private final int workerThreads;
    private final AddressPort listenBinding;

    private EventLoopGroup acceptorGroup;
    private EventLoopGroup workerGroup;

    private final FluentChannelInitializer channelInitializer;

    private final NamedThreadFactory nioThreadFactory;

    @Inject
    ServerImpl(NetworkServerConfigProvider config,
               ESConfigProvider esConfigProvider,
               CommandRouter commandRouter) {

        this.esConfigProvider = esConfigProvider;
        this.commandRouter = commandRouter;

        this.bootstrap = new ServerBootstrap();

        this.acceptorThreads = config.getAcceptorThreads();
        this.workerThreads   = config.getWorkerThreads();
        this.listenBinding   = config.getListenBinding();

        this.nioThreadFactory = new NamedThreadFactory("NabuServerNIO");

        this.acceptorGroup = new NioEventLoopGroup(acceptorThreads, nioThreadFactory.childFactory("NabuSrvAcceptor"));
        this.workerGroup   = new NioEventLoopGroup(workerThreads, nioThreadFactory.childFactory("NabuSrvWorker"));

        this.channelInitializer =
                new FluentChannelInitializer()
                        .addHandler(ResponseEncoder.class)
                        .addHandler(CommandDecoder.class)
                        .addHandler(NabuCommandInboundHandler.class,
                                new Object[]{ esConfigProvider,       commandRouter},
                                new Class[] { ESConfigProvider.class, CommandRouter.class });
    }

    @Override
    public void start() throws ComponentException {
        bootstrap.group(acceptorGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true) // the nabu protocol is tiny. John Nagle is not our friend.
         .handler(new LoggingHandler())
         .childHandler(channelInitializer);

        logger.info("Binding NettyServer on {}:{}, with {} acceptor thread(s) and {} worker thread(s)",
                listenBinding.getAddress(), listenBinding.getPort(), acceptorThreads, workerThreads);

        try {
            this.listenerChannel = bootstrap.bind(listenBinding.getAddress(), listenBinding.getPort())
                                            .sync()
                                            .channel();
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
