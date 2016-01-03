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
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.net.FluentChannelInitializer;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * A client for the Enki protocol, used by Nabu.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class EnkiClientImpl extends EnkiClient implements EnkiClientEventListener {
    private final FluentChannelInitializer channelInitializer;
    private EnkiAddressProvider config;
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;
    private Channel clientChannel;

    @Delegate(types=EnkiClientEventSource.class)
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
        this.dispatcher = new EnkiClientEventDispatcher(this);

        this.dispatcher.addEnkiClientEventListener(this);
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
    public boolean onConfigurationReceived(EnkiConnection enki, Map<String, Serializable> config) {
        logger.info("Received configuration from Enki! {}", config);
        synchronized ($enkiSourcedConfigLock) {
            wasEnkiSourcedConfigSet = true;
            enkiSourcedConfigs = ImmutableMap.copyOf(config);
        }

        return true;
    }

    @Override
    public boolean onConnectionLost(EnkiConnection enki, boolean wasLeaving, boolean serverInitiated, boolean wasAcked) {
        /* no-op, but should find the next master unless its supposed to be leaving. */
        return true;
    }

}
