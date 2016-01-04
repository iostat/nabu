package io.stat.nabuproject.core.enkiprotocol.client;

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
import io.stat.nabuproject.core.enkiprotocol.EnkiAddressProvider;
import io.stat.nabuproject.core.enkiprotocol.EnkiSourcedConfigKeys;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventDispatcher;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventListener;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventSource;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.net.FluentChannelInitializer;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final AtomicBoolean shouldAttemptReconnect;

    @Inject
    public EnkiClientImpl(EnkiAddressProvider provider) {
        this.shouldAttemptReconnect = new AtomicBoolean(true);

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

    public void beginClientReconnectLoop() throws ComponentException {
        int loops = 1;
        String lastEnkiHostSeen = "";
        int lastEnkiPortSeen = -1;
        int sameHostSeenCount = 0;
        while(shouldAttemptReconnect.get()) {
            if(!config.isEnkiDiscovered()) {
                shouldAttemptReconnect.set(false);
                throw new ComponentException(true, "EnkiAddressProvider did not discover any Enkis!");
            }

            String thisEnkiHost = config.getEnkiHost();
            int thisEnkiPort = config.getEnkiPort();

            if(lastEnkiHostSeen.equals(thisEnkiHost) && lastEnkiPortSeen == thisEnkiPort) {
                sameHostSeenCount++;

                if(sameHostSeenCount == 5) {
                    // todo: also a gargantuan fucking hack.
                    throw new ComponentException(true, "Can't find any Enki hosts that I haven't already seen.");
                }
                // we may have just lost the connection, lets a wait bit for the cluster state to
                // to get updated.
                // todo: total fucking hack until we can get a proper system of enki master election going.
                try {
                    Thread.sleep(1000);
                } catch(Exception e) {
                    logger.info("cant sleep #storyofmylife");
                    continue;
                }

                continue;
            }

            // todo: can anyone say hack?

            lastEnkiHostSeen = thisEnkiHost;
            lastEnkiPortSeen = thisEnkiPort;

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

                clientChannel.closeFuture().syncUninterruptibly();
            } catch(Exception e) {
                shouldAttemptReconnect.set(false);
                // this is an unchecked exception... but it could still be there
                if(e instanceof ConnectException) {
                    logger.error("Got a ConnectException! ", e);
                } else if(e instanceof InterruptedException) {
                    this.eventLoopGroup.shutdownGracefully();

                    logger.error("Failed to " + (loops == 1 ? "" : "re") + "start EnkiClient, {}", e);
                    throw new ComponentException(true, e);
                }
            }

            logger.info("restarting of the loop");
            loops++;
        }
    }

    private void closeClientWithoutShutdown() {
        this.clientChannel.close();
    }

    @Override
    public void start() throws ComponentException {
        try {
            beginClientReconnectLoop();
        } catch(ComponentException ce) {
            logger.info("Reconnect loop stopped!", ce);
            shutdown();
            throw ce;
        }
        logger.info("past start!");
    }

    @Override
    public void shutdown() throws ComponentException {
        dispatcher.shutdown();

        if(this.clientChannel != null) { this.clientChannel.close(); }
        if(this.eventLoopGroup != null) { this.eventLoopGroup.shutdownGracefully(); }
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
        // todo: figure out a way to find the next enki master and connect to that!
        logger.info("onConnectionLost!");
        closeClientWithoutShutdown();
        logger.info("ccws!");
        return true;
    }
}
