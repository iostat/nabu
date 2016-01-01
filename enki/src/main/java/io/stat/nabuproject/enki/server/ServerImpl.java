package io.stat.nabuproject.enki.server;

import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.enkiprotocol.EnkiChannelInitializer;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiHeartbeat;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacketType;
import io.stat.nabuproject.enki.EnkiConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Runs the Enki management server.
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

    @Inject
    ServerImpl(EnkiConfig config) {
        this.bootstrap = new ServerBootstrap();

        this.acceptorThreads = config.getAcceptorThreads();
        this.workerThreads   = config.getWorkerThreads();
        this.bindAddress     = config.getListenAddress();
        this.bindPort        = config.getListenPort();

        this.acceptorGroup = new NioEventLoopGroup(acceptorThreads);
        this.workerGroup = new NioEventLoopGroup(workerThreads);
    }

    @Override
    public void start() throws ComponentException {
        bootstrap.group(acceptorGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true) // the enki protocol is really tiny. john nagle is not our friend.
                .handler(new LoggingHandler())
                .childHandler(new EnkiChannelInitializer(EnkiServerHandler.class));

        logger.info("Binding NettyServer on {}:{}, with {} acceptor thread(s) and {} worker thread(s)",
                bindAddress, bindPort, acceptorThreads, workerThreads);

        try {
            this.listenerChannel = bootstrap.bind(bindAddress, bindPort).sync().channel();
        } catch(InterruptedException e) {
            this.acceptorGroup.shutdownGracefully();
            this.workerGroup.shutdownGracefully();

            logger.error("Failed to start NettyServer, {}", e);
            throw new ComponentException(true, e);
        }
    }

    @Override
    public void shutdown() throws ComponentException {
        // TODO: fixes for shutdown() called on failed initialization (not critical)
        logger.info("Shutting down Enki (over Netty)...");
        this.listenerChannel.close();

        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    /**
     * The name is a bit misleading, as this <i>technically</i> handles a client (meaning, it
     * responds to packets sent by the client.)
     */
    public static class EnkiServerHandler extends SimpleChannelInboundHandler<EnkiPacket> {
        public EnkiServerHandler() { super(); }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            logger.info("CHANNEL_ACTIVE: {}", ctx);
            ctx.writeAndFlush(new EnkiHeartbeat(0));
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.error("CHANNEL_INACTIVE");
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("EXCEPTION_CAUGHT: {}", cause);
            super.exceptionCaught(ctx, cause);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, EnkiPacket msg) throws Exception {
            logger.info("channelRead0: {}", msg);
            if(msg.getType() == EnkiPacketType.ACK) {
                logger.info("ack!");
            }
        }
    }
}
