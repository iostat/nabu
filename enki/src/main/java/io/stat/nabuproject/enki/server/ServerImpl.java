package io.stat.nabuproject.enki.server;

import com.google.inject.Inject;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.net.FluentChannelInitializer;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.core.util.AsyncListenerDispatcher;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import io.stat.nabuproject.core.util.functional.PentaConsumer;
import io.stat.nabuproject.core.util.functional.QuadConsumer;
import io.stat.nabuproject.core.util.functional.TriConsumer;
import io.stat.nabuproject.enki.EnkiConfig;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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

    @Override
    public void addNabuConnectionListener(NabuConnectionListener ncl) {
        dispatcher.addNabuConnectionListener(ncl);
    }

    @Override
    public void removeNabuConnectionListener(NabuConnectionListener ncl) {
        dispatcher.removeNabuConnectionListener(ncl);
    }

    /**
     * Dispatches callbacks to any {@link NabuConnectionListener}s registered to it asynchronously.
     */
    @Slf4j
    private static final class NabuConnectionListenerDispatcher implements NabuConnectionListener {
        @Delegate(types=Component.class)
        private final AsyncListenerDispatcher<NabuConnectionListener> dispatcher;

        public NabuConnectionListenerDispatcher() {
            // todo: figure out optimal thread pool sized because
            // honestly these thread pool sizes are beyond overkill
            // ditto for the timeouts
            // especially for the collector
            // (it has to be larger than the worker timeout
            //  though so that the workers it's collecting against
            //  can timeout without causing the collector to timeout)

            // todo: make these configurable?
            // 5 min ThreadPool size
            // 20 max threads
            // 60 second timeout
            // backed by a SynchronousQueue.
            // with thread names starting with NCLDWorker
            ExecutorService dispatchWorkerExecutor = new ThreadPoolExecutor(
                    5, 60,
                    60, TimeUnit.SECONDS,
                    new SynchronousQueue<>(),
                    new NamedThreadFactory("NabuCnxnDispatcher-Worker")
            );

            // 20 fixed pool size.
            // 5 minute timeout
            // with thread names starting with NCLDCollector
            ExecutorService collectorWorkerExecutor = new ThreadPoolExecutor(
                    20, 20,
                    5, TimeUnit.MINUTES,
                    new SynchronousQueue<>(),
                    new NamedThreadFactory("NabuCnxnDispatcher-Collector")
            );

            this.dispatcher = new AsyncListenerDispatcher<>(dispatchWorkerExecutor, collectorWorkerExecutor);
        }


        @Override
        public void onNewNabuConnection(NabuConnection cnxn) {
            logger.info("onNewNabuConnection({})", cnxn);
            dispatchBinaryKillerTask("onNewNabuConnection", NabuConnectionListener::onNewNabuConnection, cnxn);
        }

        @Override
        public void onNabuLeaving(NabuConnection cnxn, boolean serverInitiated) {
            logger.info("onNabuLeaving({}, {})", cnxn, serverInitiated);
            dispatchTernaryKillerTask("onNabuLeaving", NabuConnectionListener::onNabuLeaving, cnxn, serverInitiated);
        }

        @Override
        public void onPacketDispatched(NabuConnection cnxn, EnkiPacket packet, CompletableFuture<EnkiPacket> future) {
            logger.info("onPacketDispatched({}, {}, {})", cnxn, packet, future);
            dispatchQuaternaryKillerTask(
                    "onPacketDispatched",
                    NabuConnectionListener::onPacketDispatched,
                    cnxn, packet, future
            );
        }

        @Override
        public void onNabuDisconnected(NabuConnection cnxn, boolean wasLeaving, boolean serverInitiated, boolean wasAcked) {
            logger.info("onNabuDisconnected({}, {}, {}, {}", cnxn, wasLeaving, serverInitiated, wasAcked);
            dispatchPentaryKillerTask(
                    "onNabuDisconnected",
                    NabuConnectionListener::onNabuDisconnected,
                    cnxn,
                    wasLeaving,
                    serverInitiated,
                    wasAcked);
        }

        @Override
        public void onPacketReceived(NabuConnection cnxn, EnkiPacket packet) {
            logger.info("onPacketReceived({}, {})", cnxn, packet);
            dispatchBinaryAckerTask(
                    "onPacketReceived",
                    NabuConnectionListener::onPacketReceived,
                    cnxn,
                    packet
            );
        }

        public void addNabuConnectionListener(NabuConnectionListener listener) {
            dispatcher.addListener(listener);
        }

        public void removeNabuConnectionListener(NabuConnectionListener listener) {
            dispatcher.removeListener(listener);
        }

        private <T extends NabuConnection, U extends EnkiPacket> void dispatchBinaryAckerTask(String callbackName,
                                                                                              TriConsumer<
                                                                                                      NabuConnectionListener,
                                                                                                      T, U> callback,
                                                                                              T cnxn, U packet) {
            dispatcher.dispatchListenerCallbacks(listener -> callback.accept(listener, cnxn, packet),
                    new CBFailedPacketNAKer(
                        callbackName,
                        cnxn,
                        packet));
        }

        private void dispatchKillerTask(String callbackName, NabuConnection cnxn, Consumer<NabuConnectionListener> listenerConsumer) {
            dispatcher.dispatchListenerCallbacks(listenerConsumer, new CBFailedCnxnKicker(callbackName, cnxn));
        }

        private <T extends NabuConnection> void dispatchBinaryKillerTask(String callbackName,
                                                                         BiConsumer<
                                                                                 NabuConnectionListener,
                                                                                 T> callback,
                                                                         T cnxn) {
            dispatchKillerTask(callbackName, cnxn, listener -> callback.accept(listener, cnxn));
        }

        private <T extends NabuConnection, U> void dispatchTernaryKillerTask(String callbackName,
                                                                             TriConsumer<
                                                                                     NabuConnectionListener,
                                                                                     T,
                                                                                     U
                                                                                     > callback,
                                                                             T cnxn, U arg2) {
            dispatchKillerTask(callbackName, cnxn, listener -> callback.accept(listener, cnxn, arg2));
        }

        private <T extends NabuConnection, U , V> void dispatchQuaternaryKillerTask(String callbackName,
                                                                                    QuadConsumer<
                                                                                            NabuConnectionListener,
                                                                                            T, U, V> callback,
                                                                                    T cnxn, U arg2, V arg3) {
            dispatchKillerTask(callbackName, cnxn, listener -> callback.accept(listener, cnxn, arg2, arg3));
        }

        private <T extends NabuConnection, U, V, W> void dispatchPentaryKillerTask(String callbackName,
                                                                                   PentaConsumer<
                                                                                           NabuConnectionListener,
                                                                                           T, U, V, W
                                                                                           > callback,
                                                                                   T cnxn, U arg2, V arg3, W arg4) {
            dispatchKillerTask(callbackName, cnxn, listener -> callback.accept(listener, cnxn, arg2, arg3, arg4));
        }


        private static final class CBFailedCnxnKicker extends AsyncListenerDispatcher.CallbackResultsConsumer {
            private final String name;
            private final NabuConnection cnxn;

            CBFailedCnxnKicker(String collectionType,
                               NabuConnection cnxn) {
                this.name = String.format("%s-%s", cnxn.prettyName(), collectionType);
                this.cnxn = cnxn;
            }

            @Override
            public void failedWithThrowable(Throwable t) {
                logger.error("Received an Exception while collecting " + name, t);
                cnxn.kick();
            }

            @Override
            public void failed() {
                logger.error("Some dispatch tasks failed for {}", name);
                cnxn.kick();
            }

            @Override
            public void success() { /* no-op */ }
        }


        private static final class CBFailedPacketNAKer extends AsyncListenerDispatcher.CallbackResultsConsumer {
            private final String name;
            private final NabuConnection cnxn;
            private final EnkiPacket packet;

            CBFailedPacketNAKer(String collectionType,
                                NabuConnection cnxn,
                                EnkiPacket packet) {
                this.name = String.format("%s-%s::%s", cnxn.prettyName(), collectionType, packet);
                this.packet = packet;
                this.cnxn = cnxn;
            }

            @Override
            public void failedWithThrowable(Throwable t) {
                logger.error("Received an Exception while collecting callbacks for packet " + name, t);
                cnxn.nak(packet);
            }

            @Override
            public void failed() {
                logger.error("Some dispatch tasks failed for " + name);
                cnxn.nak(packet);
            }

            @Override
            public void success() {
                cnxn.ack(packet);
            }
        }
    }
}
