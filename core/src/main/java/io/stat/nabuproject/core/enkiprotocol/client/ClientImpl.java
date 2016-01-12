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
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiConfigure;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.net.channel.FluentChannelInitializer;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A client for the Enki protocol, used by Nabu.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class ClientImpl extends EnkiClient implements EnkiClientEventListener {
    private final FluentChannelInitializer channelInitializer;
    private RetryTrackingAddressProvider provider;
    private EventLoopGroup eventLoopGroup;
    private Channel clientChannel;

    @Delegate(types=EnkiClientEventSource.class)
    private final EnkiClientEventDispatcher dispatcher;

    private final byte[] $enkiSourcedConfigLock;
    private boolean wasEnkiSourcedConfigSet;
    private Map<String, Object> enkiSourcedConfigs;

    private final AtomicBoolean shouldAttemptReconnect;
    private final AtomicBoolean wasRedirected;
    private final AtomicReference<AddressPort> redirectedTo;

    private final Thread reconnector;

    @Inject
    public ClientImpl(EnkiAddressProvider provider) {
        this.shouldAttemptReconnect = new AtomicBoolean(true);
        this.wasRedirected          = new AtomicBoolean(false);
        this.redirectedTo           = new AtomicReference<>(null);

        this.$enkiSourcedConfigLock = new byte[0];
        this.wasEnkiSourcedConfigSet = false;
        this.enkiSourcedConfigs = ImmutableMap.of();

        this.provider = new RetryTrackingAddressProvider(provider);
        this.eventLoopGroup = new NioEventLoopGroup();
        this.dispatcher = new EnkiClientEventDispatcher(this);

        this.dispatcher.addEnkiClientEventListener(this);
        this.channelInitializer =
                new FluentChannelInitializer()
                        .addHandler(EnkiPacket.Encoder.class)
                        .addHandler(EnkiPacket.Decoder.class)
                        .addHandler(
                                EnkiClientIO.class,
                                new Object[] { this, dispatcher },
                                new Class[] { EnkiClient.class, EnkiClientEventListener.class }
                        );

        this.reconnector = new Thread(this::clientReconnectLoop);
        this.reconnector.setName("EnkiClientReconnector");
        this.reconnector.setUncaughtExceptionHandler((t, e) -> {
            logger.warn("Reconnect loop stopped!", e);
            shutDownEverything();
        });

        addEnkiClientEventListener(this);
    }

    public void clientReconnectLoop() throws ComponentException {
        ComponentException noMoreEnkis = new ComponentException(true, "Could not find any eligible Enkis to connect to!");
        while(shouldAttemptReconnect.get()) {
            AddressPort next;
            if(wasRedirected.get()) {
                next = redirectedTo.get();
                wasRedirected.set(false);
                redirectedTo.set(null);
            } else {
                if(!provider.isEnkiDiscovered()) {
                    throw noMoreEnkis;
                }
                next = provider.getNextEnki();
            }

            if(next == null) {
                throw noMoreEnkis;
            }

            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true) // the enki protocol is really tiny. john nagle is not our friend.
                    .handler(channelInitializer);

            try {
                this.clientChannel = bootstrap
                        .connect(next.getAddress(), next.getPort())
                        .sync()
                        .channel();

                this.clientChannel.closeFuture().sync();
            } catch(Exception e) {
                // this is an unchecked exception... but it could still be there
                //noinspection ConstantConditions
                if(e instanceof ConnectException) {
                    logger.error("Got a ConnectException! (Going to the next server)", e);
                    continue;
                }

                if(e instanceof InterruptedException) {
                    this.eventLoopGroup.shutdownGracefully();
                    shouldAttemptReconnect.set(false);
                    throw new ComponentException(true, e);
                } else {
                    logger.error("Unhandled exception in client reconnect loop!", e);
                    shutdown();
                }
            } finally {
                this.clientChannel = null;
            }
        }
    }

    private void closeClientWithoutShutdown() {
        if (this.clientChannel != null) {
            this.clientChannel.close();
        } else {
            logger.warn("Tried to closeClientWithoutShutdown on a null clientChannel!");
        }
    }

    @Override
    public void start() throws ComponentException {
        reconnector.start();
    }

    @Override
    public void shutdown() throws ComponentException {
        dispatcher.shutdown();

        if(this.clientChannel != null) { this.clientChannel.close().syncUninterruptibly(); }
        if(this.eventLoopGroup != null) { this.eventLoopGroup.shutdownGracefully(); }

        try {
            reconnector.join();
        } catch (InterruptedException ie) {
            throw new ComponentException("InterruptedException when joining reconnector thread...", ie);
        }
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
    public boolean onConfigurationReceived(EnkiConnection enki, EnkiConfigure packet) {
        logger.info("Received configuration from Enki! {}", packet.getOptions());
        provider.connectionSuccessful();
        synchronized ($enkiSourcedConfigLock) {
            wasEnkiSourcedConfigSet = true;
            enkiSourcedConfigs = ImmutableMap.copyOf(packet.getOptions());
        }

        return true;
    }

    @Override
    public boolean onConnectionLost(EnkiConnection enki,
                                    EnkiConnection.DisconnectCause cause,
                                    boolean wasAcked) {
        if (cause != EnkiConnection.DisconnectCause.REDIRECT) {
            closeClientWithoutShutdown();
        }

        return true;
    }

    @Override
    void setRedirectionTarget(AddressPort ap) {
        logger.info("Being redirected to: {}", ap);
        wasRedirected.set(true);
        redirectedTo.set(ap);
    }

    @Override
    void shutDownEverything() {
        if(this.getStarter() != null) {
            this.getStarter().shutdown();
            // todo: figure out a better to way to SHUT DOWN EVERYTHING
        }
    }
}
