package io.stat.nabuproject.nabu.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.net.channel.FluentChannelInitializer;
import io.stat.nabuproject.core.util.CatastrophicException;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import io.stat.nabuproject.nabu.protocol.CommandEncoder;
import io.stat.nabuproject.nabu.protocol.ResponseDecoder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A high-level client for communicating with Nabu
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class NabuClient {
    /**
     * Default timeout for a connection attempt to a Nabu Server.
     * A timeout implies no response has been received when attempting to
     * identify the server.
     */
    public static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = 10000;

    /**
     * The list of servers that this client should attempt to connect to
     */
    private final @Getter ImmutableList<AddressPort> servers;

    /**
     * How long a connection attempt should wait until it is considered timed out.
     */
    private final @Getter long connectionTimeout;

    /**
     * The cluster that the Nabu we connect to operates on. This is to provide
     * a layer of protection against misconfigured or misbehaving load balancers.
     */
    private final @Getter String expectedClusterName;

    /**
     * Used internally to manage threads spawned by this Nabu Client
     */
    private final NamedThreadFactory nioThreadFactory;

    /**
     * The NIO event loop group which the client runs on.
     * Threads are provided by nioThreadFactory.
     */
    private final EventLoopGroup nioEventGroup;

    /**
     * Used internally to keep track of the state of the connection loop.
     */
    private final NabuClientConnectionState nccs;

    /**
     * Create a new NabuClient that will only try to connect to the specified address and port.
     * The connection will be considered successful if the Nabu on the other end identifies itself
     * as participating in expectedClusterName. A connection will be considered timed out if it takes
     * longer than {@link NabuClient#DEFAULT_CONNECTION_TIMEOUT_MILLIS} to establish.
     * @param expectedClusterName the ES cluster name that the Nabu should be on
     * @param address the address of the Nabu
     * @param port the port that the Nabu is bound to
     */
    public NabuClient(String expectedClusterName, String address, int port){
        this(expectedClusterName, address, port, DEFAULT_CONNECTION_TIMEOUT_MILLIS);
    }

    /**
     * Create a new NabuClient that will only try to connect to the specified address and port.
     * The connection will be considered successful if the Nabu on the other end identifies itself
     * as participating in expectedClusterName. A connection will be considered timed out if it takes
     * longer than <code>timeout</code> to establish.
     * @param expectedClusterName the ES cluster name that the Nabu should be on
     * @param address the address of the Nabu
     * @param port the port that the Nabu is bound to
     * @param timeout how long to wait for a response before the connection is considered timed out.
     */
    public NabuClient(String expectedClusterName, String address, int port, long timeout) {
        this(expectedClusterName, ImmutableList.of(new AddressPort(address, port)), timeout);
    }

    /**
     * Create a new NabuClient that expects a Nabu writing to the specified ElasticSearch cluster
     * and attempts to connect to the specified servers, with the {@link NabuClient#DEFAULT_CONNECTION_TIMEOUT_MILLIS} timeout
     * @param expectedClusterName the ES cluster name that the Nabu should be on
     * @param servers a list of {@link AddressPort}s to try connecting to.
     */
    public NabuClient(String expectedClusterName, List<AddressPort> servers) {
        this(expectedClusterName, servers, DEFAULT_CONNECTION_TIMEOUT_MILLIS);
    }

    /**
     * Create a new NabuClient that expects a Nabu writing to the specified ElasticSearch cluster
     * and attempts to connect to the specified servers, with the specified connection attempt timeout
     * @param expectedClusterName the ES cluster name that the Nabu should be on
     * @param servers a list of {@link AddressPort}s to try connecting to.
     * @param timeout how long to wait for a connection to succeed before considering it timed out.
     */
    public NabuClient(String expectedClusterName, List<AddressPort> servers, long timeout) {
        this.expectedClusterName = expectedClusterName;
        this.servers = ImmutableList.copyOf(servers);
        this.connectionTimeout = timeout;

        this.nioThreadFactory = new NamedThreadFactory("NabuClientNIO");
        this.nioEventGroup = new NioEventLoopGroup(0, this.nioThreadFactory);
        this.nccs = new NabuClientConnectionState(this);

        logger.debug("new NabuClient constructed, {}", this);
    }

    /**
     * Begin attempting to connect to the Nabus specified in the constructor.
     * If multiple servers were specified, it will wait up to {@literal (connectionTimeout + 3000) * servers.size()} milliseconds
     * for any connection to succeed. If all connections failed by that point a NabuConnectionFailedException will be thrown.
     * @throws NabuConnectionFailedException if connections could not be established.
     * @throws CatastrophicException if connections could not be established, AND we couldn't clean up after ourselves.
     */
    public void connect() throws CatastrophicException, NabuConnectionFailedException {
        synchronized (nccs) {
            Thread connectorThread = nioThreadFactory.newThread(this::connectorLoop);
            nccs.setConnectorThread(connectorThread);
            connectorThread.start();
        }

        NabuConnectionFailedException toThrowUp = null;

        boolean latchFinishedBeforeTimeout = false;

        try {
            latchFinishedBeforeTimeout = nccs.getStartupSynchronizer().await((connectionTimeout + 3000) * servers.size(), TimeUnit.MILLISECONDS);
        } catch(Exception e) {
            if (e instanceof InterruptedException) {
                toThrowUp = new NabuConnectionFailedException("connect() timed out waiting for the connector thread to finish.", e);
            } else {
                toThrowUp = new NabuConnectionFailedException("Waiting for the connection to establish failed for some inexplicable reason", e);
            }
        }

        // todo: simplify all this logic.

        if(nccs.getConnectionState() == NCCState.FAILED) {
            gracefulShutdown();
            toThrowUp  = nccs.getFailureReason();
        }

        if(nccs.getConnectionState() != NCCState.RUNNING && latchFinishedBeforeTimeout) {
            toThrowUp = nccs.getFailureReason();
        }

        if(toThrowUp != null || !latchFinishedBeforeTimeout) {
            if(toThrowUp == null) {
                toThrowUp = new NabuConnectionFailedException("NabuClient is not in a connected state, and failed to explain why not.");
            }

            gracefulShutdown();
            throw toThrowUp;
        }
    }

    private void gracefulShutdown() {
        this.nioEventGroup.shutdownGracefully();
    }

    private void passiveAggressiveShutdown() throws CatastrophicException {
        gracefulShutdown();
        nukeThreadFactory();
    }

    private void nukeThreadFactory() throws CatastrophicException {
        try {
            nioThreadFactory.nukeCreatedHeirarchy(500);
        } catch(NamedThreadFactory.ThreadNukeException tne) {
            logger.error("Failed to nuke the NIO Thread Factory, failed threads include: {}", tne.getFailures());
            throw new CatastrophicException("Could not cleanly stop NabuClient related Threads. You're on your own.", tne);
        }
    }

    public void disconnect() {
        synchronized (nccs) {
            if(nccs.getConnectionState() != NCCState.DISCONNECTED) {
                nccs.isShuttingDown(true);
                nccs.getConnectorThread().interrupt();
            }
        }

        this.nioEventGroup.shutdownGracefully();
    }

    private void connectorLoop() {
        ArrayDeque<AddressPort> addressesToTry = Queues.newArrayDeque(servers);
        synchronized (nccs) {
            nccs.setConnectionState(NCCState.CONNECTING);
        }

        while(true) {
            if(nccs.getConnectionState() == NCCState.SHUTTING_DOWN) {
                break;
            }

            if(addressesToTry.size() == 0) {
                synchronized (nccs) {
                    nccs.setConnectionState(NCCState.FAILED);
                    nccs.setFailureReason(new NabuConnectionFailedException("Ran out of addresses to connect to."));
                    return;
                }
            }

            nccs.getStartupSynchronizer().reset();

            AddressPort thisAddressPortAttempt = addressesToTry.pop();

            FluentChannelInitializer fci = new FluentChannelInitializer()
                                                .addHandler(new CommandEncoder())
                                                .addHandler(new ResponseDecoder())
                                                .addHandler(NabuClientIO.class,
                                                            new Object[] { nccs, expectedClusterName },
                                                            new Class[]  { HighLevelNabuClientBridge.class, String.class}
                                                )
                                                .addHandler(new LoggingHandler("NabuClientNettyInternal", LogLevel.DEBUG));

            try {
                Bootstrap bootstrap = new Bootstrap();
                bootstrap.group(nioEventGroup)
                         .option(ChannelOption.TCP_NODELAY, true) // todo: potentially dangerous downcoercion in CONNECT_TIMEOUT_MILLIS
//                         .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, ((int)this.connectionTimeout))
                         .channel(NioSocketChannel.class)
                         .handler(fci);

                ChannelFuture connectFuture = bootstrap.connect(thisAddressPortAttempt.toInetSocketAddress());
                Channel theChannel;

                try {
                    theChannel = connectFuture.sync().channel();
                } catch (Exception e) {
                    logger.error("Failed to connect to " + thisAddressPortAttempt + ", restarting connect loop", e);
                    nccs.setConnectionState(NCCState.RETRYING);
                    continue;
                }

                nccs.setClientChannel(theChannel);
                nccs.setConnectionState(NCCState.IDENTIFYING);

                boolean establishHit = nccs.getStartupSynchronizer().await(connectionTimeout, TimeUnit.MILLISECONDS);

                if(establishHit) {
                    NCCState state = nccs.getConnectionState();
                    if(state == NCCState.FAILED) {
                        logger.error("Connection established but ES cluster ID didn't match expected for {}", thisAddressPortAttempt);
                        theChannel.close().await();
                    } else if(state == NCCState.RUNNING) {
                        logger.info("Successfully established a connection to {}.", thisAddressPortAttempt);
                        // reset all addresses, so if it has to reconnect it can retry everybody
                        theChannel.closeFuture().sync();

                        if(nccs.isShuttingDown()) {
                            return;
                        } else {
                            addressesToTry = Queues.newArrayDeque(servers);
                            continue;
                        }
                    } else if (state == NCCState.IDENTIFYING) {
                        logger.info("Server at {}, did not identify itself before the connection timeout. Killing connection.");
                        nccs.setConnectionState(NCCState.RETRYING);
                        theChannel.close().await();
                    } else {
                        logger.error("Client is in an impossible state. Killing connection and leaving.");
                        theChannel.close().await();
                        disconnect();
                        return;
                    }
                } else {
                    logger.error("Connection to {} did not successfully establish and identify before the timeout. Killing it.", thisAddressPortAttempt);
                    nccs.setConnectionState(NCCState.RETRYING);
                    theChannel.close().sync();
                }
            } catch(InterruptedException e) {
                if(!nccs.isShuttingDown()) {
                    logger.warn("Received an InterruptException, likely from a connection timeout.", e);
                } else {
                    logger.warn("Received an InterruptException and I am not shutting down...", e);
                    disconnect();
                }
            }
        }
    }
}
