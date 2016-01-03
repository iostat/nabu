package io.stat.nabuproject.core.enkiprotocol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.core.util.AsyncListenerDispatcher;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A client for the Enki protocol, used by Nabu.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class EnkiClientImpl extends EnkiClient implements EnkiClientEventListener {

    private final FluentChannelInitializer channelInitializer;
    private EnkiAddressProvider config;
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;
    private Channel clientChannel;

    private final EnkiClientEventDispatcher dispatcher;

    private final byte[] $enkiSourcedConfigLock;
    private boolean wasEnkiSourcedConfigSet;
    private Map<String, Object> enkiSourcedConfigs;

    @Inject
    public EnkiClientImpl(EnkiAddressProvider provider) {
        this.$enkiSourcedConfigLock = new byte[0];
        this.wasEnkiSourcedConfigSet = false;
        this.enkiSourcedConfigs = ImmutableMap.of();

        this.config = provider;
        this.eventLoopGroup = new NioEventLoopGroup();
        this.dispatcher = new EnkiClientEventDispatcher();
        this.channelInitializer =
                new FluentChannelInitializer()
                        .addHandler(EnkiPacket.Encoder.class)
                        .addHandler(EnkiPacket.Decoder.class)
                        .addHandler(
                                EnkiClientIO.class,
                                new Object[] { dispatcher },
                                new Class[] { EnkiClientEventListener.class }
                        );

        addEnkiClientEventListener(this);
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
        dispatcher.shutdown();
        this.clientChannel.close();
        this.eventLoopGroup.shutdownGracefully();
    }

    @Override
    public void addEnkiClientEventListener(EnkiClientEventListener ecel) {
        dispatcher.addListener(ecel);
    }

    @Override
    public void removeEnkiClientEventListener(EnkiClientEventListener ecel) {
        dispatcher.removeListener(ecel);
    }

    @Override
    public boolean isKafkaBrokerConfigAvailable() {
        synchronized($enkiSourcedConfigLock) {
            return wasEnkiSourcedConfigSet;
        }
    }

    @Override @SuppressWarnings("unchecked")
    public List<String> getKafkaBrokers() {
        synchronized ($enkiSourcedConfigLock) {
            return (List<String>) enkiSourcedConfigs.getOrDefault(EnkiSourcedConfigKeys.KAFKA_BROKERS, ImmutableList.of());
        }
    }

    @Override
    public String getKafkaGroup() {
        synchronized($enkiSourcedConfigLock) {
            return enkiSourcedConfigs.getOrDefault(EnkiSourcedConfigKeys.KAFKA_GROUP, "").toString();
        }
    }

    @Override @SuppressWarnings("unchecked")
    public List<ThrottlePolicy> getThrottlePolicies() {
        return (List<ThrottlePolicy>) enkiSourcedConfigs.getOrDefault(EnkiSourcedConfigKeys.THROTTLE_POLICIES, ImmutableList.of());
    }

    @Override
    public void onConfigurationReceived(EnkiConnection enki, Map<String, Serializable> config) {
        logger.info("Received configuration from Enki! {}", config);
        synchronized ($enkiSourcedConfigLock) {
            wasEnkiSourcedConfigSet = true;
            enkiSourcedConfigs = ImmutableMap.copyOf(config);
        }
    }

    @Override
    public void onTaskAssigned(EnkiConnection enki, TopicPartition topicPartition) { /* no-op */ }

    @Override
    public void onTaskUnassigned(EnkiConnection enki, TopicPartition topicPartition) { /* no-op */ }

    @Override
    public void onConnectionLost(EnkiConnection enki, boolean wasLeaving, boolean serverInitiated, boolean wasAcked) { /* no-op, but should find the next master unless its supposed to be leaving. */ }

    private final class EnkiClientEventDispatcher implements EnkiClientEventListener {
        @Delegate(types=Component.class)
        private final AsyncListenerDispatcher<EnkiClientEventListener> dispatcher;
        private final AsyncListenerDispatcher.CallbackResultsConsumer CALLBACK_FAILED_SHUTDOWNER;
        private final Logger logger;

        public EnkiClientEventDispatcher() {
            // todo: figure out optimal thread pool sized because
            // honestly these thread pool sizes are beyond overkill
            // ditto for the timeouts
            // especially for the collector
            // (it has to be larger than the worker timeout
            //  though so that the workers it's collecting against
            //  can timeout without causing the collector to timeout)

            // todo: make these configurable?
            // 5 min ThreadPool size
            // 30 max threads
            // 60 second timeout
            // backed by a SynchronousQueue.
            // with thread names starting with NCLDWorker
            ExecutorService dispatchWorkerExecutor = new ThreadPoolExecutor(
                    5, 30,
                    60, TimeUnit.SECONDS,
                    new SynchronousQueue<>(),
                    new NamedThreadFactory("EnkiClientEventDispatcher-Worker")
            );

            // 20 fixed pool size.
            // 5 minute timeout
            // with thread names starting with NCLDCollector
            ExecutorService collectorWorkerExecutor = new ThreadPoolExecutor(
                    20, 20,
                    5, TimeUnit.MINUTES,
                    new SynchronousQueue<>(),
                    new NamedThreadFactory("EnkiClientEventDispatcher-Collector")
            );

            this.CALLBACK_FAILED_SHUTDOWNER = new AsyncListenerDispatcher.CallbackResultsConsumer() {
                @Override
                public void failedWithThrowable(Throwable t) {
                    logger.error("Shutting down because some callback(s) failed with", t);
                    EnkiClientImpl.this.shutdown();
                }

                @Override
                public void failed() {
                    logger.error("Callback(s) failed with no throwable thrown. Shutting down.");
                    EnkiClientImpl.this.shutdown();
                }

                @Override
                public void success() {/* no-op */ }
            };

            this.logger = LoggerFactory.getLogger(EnkiClientImpl.EnkiClientEventDispatcher.class);
            this.dispatcher = new AsyncListenerDispatcher<>(dispatchWorkerExecutor, collectorWorkerExecutor);
        }

        @Override
        public void onConfigurationReceived(EnkiConnection enki, Map<String, Serializable> config) {
            logger.info("onConfigurationReceived({}, {})", enki, config);
            dispatcher.dispatchListenerCallbacks(
                    listener -> listener.onConfigurationReceived(enki, config),
                    CALLBACK_FAILED_SHUTDOWNER);
        }

        @Override
        public void onTaskAssigned(EnkiConnection enki, TopicPartition topicPartition) {
            logger.info("onTaskAssigned({}, {})", enki, topicPartition);
            dispatcher.dispatchListenerCallbacks(
                    listener -> listener.onTaskAssigned(enki, topicPartition),
                    CALLBACK_FAILED_SHUTDOWNER);
        }

        @Override
        public void onTaskUnassigned(EnkiConnection enki, TopicPartition topicPartition) {
            logger.info("onUnTaskAssigned({}, {})", enki, topicPartition);
            dispatcher.dispatchListenerCallbacks(
                    listener -> listener.onTaskUnassigned(enki, topicPartition),
                    CALLBACK_FAILED_SHUTDOWNER);
        }

        @Override
        public void onConnectionLost(EnkiConnection enki, boolean wasLeaving, boolean serverInitiated, boolean wasAcked) {
            logger.info("onConnectionLost({}, {}, {}, {})", enki, wasLeaving, serverInitiated, wasAcked);
            dispatcher.dispatchListenerCallbacks(
                    listener -> listener.onConnectionLost(enki, wasLeaving, serverInitiated, wasAcked),
                    CALLBACK_FAILED_SHUTDOWNER);

        }

        public void addListener(EnkiClientEventListener ecel) {
            dispatcher.addListener(ecel);
        }

        public void removeListener(EnkiClientEventListener ecel) {
            dispatcher.removeListener(ecel);
        }
    }
}
