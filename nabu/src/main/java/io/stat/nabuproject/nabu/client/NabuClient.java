package io.stat.nabuproject.nabu.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.net.channel.FluentChannelInitializer;
import io.stat.nabuproject.core.util.CatastrophicException;
import io.stat.nabuproject.core.util.concurrent.NamedThreadFactory;
import io.stat.nabuproject.nabu.protocol.CommandEncoder;
import io.stat.nabuproject.nabu.protocol.Limits;
import io.stat.nabuproject.nabu.protocol.ResponseDecoder;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

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
    private final @Getter AddressPort server;

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
        this(expectedClusterName, new AddressPort(address, port));
    }

    /**
     * Create a new NabuClient that will only try to connect to the specified address and port.
     * The connection will be considered successful if the Nabu on the other end identifies itself
     * as participating in expectedClusterName. A connection will be considered timed out if it takes
     * longer than {@link NabuClient#DEFAULT_CONNECTION_TIMEOUT_MILLIS} to establish.
     * @param expectedClusterName the ES cluster name that the Nabu should be on
     * @param ap an {@link AddressPort} containing the server's information.
     */
    public NabuClient(String expectedClusterName, AddressPort ap) {
        this(expectedClusterName, ap, DEFAULT_CONNECTION_TIMEOUT_MILLIS);
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
        this(expectedClusterName, new AddressPort(address, port), timeout);
    }

    /**
     * Create a new NabuClient that expects a Nabu writing to the specified ElasticSearch cluster
     * and attempts to connect to the specified servers, with the specified connection attempt timeout
     * @param expectedClusterName the ES cluster name that the Nabu should be on
     * @param server the AddressPort to connect to.
     * @param timeout how long to wait for a connection to succeed before considering it timed out.
     */
    public NabuClient(String expectedClusterName, AddressPort server, long timeout) {
        this.expectedClusterName = expectedClusterName;
        this.server = server;
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
            latchFinishedBeforeTimeout = nccs.getStartupSynchronizer().await(connectionTimeout, TimeUnit.MILLISECONDS);
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

    /**
     * Attempts to perform a graceful shutdown.
     * todo: this is probably not everything necessary for a graceful shutdown...
     */
    private void gracefulShutdown() {
        this.nioEventGroup.shutdownGracefully();
    }

    /**
     * performs a gracefulShutdown(), then nukeThreadFactory()
     * @throws CatastrophicException
     */
    private void passiveAggressiveShutdown() throws CatastrophicException {
        gracefulShutdown();
        nukeThreadFactory();
    }

    /**
     *
     * @throws CatastrophicException
     */
    private void nukeThreadFactory() throws CatastrophicException {
        try {
            nioThreadFactory.nukeCreatedHeirarchy(500);
        } catch(NamedThreadFactory.ThreadNukeException tne) {
            logger.error("Failed to nuke the NIO Thread Factory, failed threads include: {}", tne.getFailures());
            throw new CatastrophicException("Could not cleanly stop NabuClient related Threads. You're on your own.", tne);
        }
    }

    /**
     * If the connection loop is still running, tell it to stop running, then shutdown everything else gracefully
     */
    public void disconnect() {
        synchronized (nccs) {
            if(nccs.getConnectionState() != NCCState.DISCONNECTED) {
                nccs.isShuttingDown(true);
            } else {
                logger.warn("disconnect() called multiple times in a row...");
                return;
            }
        }

        nccs.getConnectorThread().interrupt();

        try {
            nccs.getConnectorThread().join(10000);
        } catch (InterruptedException e) {
            logger.warn("Interrupted while attempting to join() on the connection thread.", e);
        }

        gracefulShutdown();
    }

    /**
     * Runs the reconnection loop and manages the NabuClient state machine
     */
    private void connectorLoop() {
        synchronized (nccs) {
            nccs.setConnectionState(NCCState.CONNECTING);
        }

        nccs.getStartupSynchronizer().reset();

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
                     .option(ChannelOption.SO_RCVBUF, Limits.BUFFER_LIMIT)
                     .option(ChannelOption.SO_SNDBUF, Limits.BUFFER_LIMIT)
                     .option(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(Limits.BUFFER_LIMIT)) // todo: BUFFER_LIMIT is 50 MB, which is probably beyond excessive. figure out max size of doc.
                     .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, ((int)this.connectionTimeout))
                     .channel(NioSocketChannel.class)
                     .handler(fci);

            ChannelFuture connectFuture = bootstrap.connect(server.toInetSocketAddress());
            Channel theChannel;

            try {
                theChannel = connectFuture.sync().channel();
            } catch (Exception e) {
                String msg = "Failed to connect to " + server;
                logger.error(msg, e);
                nccs.setFailureReason(new NabuConnectionFailedException(msg, e));
                nccs.setConnectionState(NCCState.FAILED);
                return;
            }

            nccs.setClientChannel(theChannel);
            nccs.setConnectionState(NCCState.IDENTIFYING);

            boolean establishHit = nccs.getStartupSynchronizer().await(connectionTimeout, TimeUnit.MILLISECONDS);

            if(establishHit) {
                NCCState state = nccs.getConnectionState();
                if(state == NCCState.FAILED) {
                    logger.error("Connection established but ES cluster ID didn't match expected for {}", server);
                    theChannel.close().await();
                } else if(state == NCCState.RUNNING) {
                    logger.info("Successfully established a connection to {}.", server);
                    // reset all addresses, so if it has to reconnect it can retry everybody
                    theChannel.closeFuture().sync();

                    if(!nccs.isShuttingDown()) {
                        logger.error("Lost connection but wasn't shutting down...");
                    }
                } else if (state == NCCState.IDENTIFYING) {
                    logger.info("Server at {}, did not identify itself before the connection timeout. Killing connection.");
                    theChannel.close().await();
                } else {
                    logger.error("Client is in an impossible state. Killing connection and leaving.");
                    theChannel.close().await();
                    disconnect();
                }
            } else {
                logger.error("Connection to {} did not successfully establish and identify before the timeout. Killing it.", server);
                theChannel.close().sync();
            }
        } catch(InterruptedException e) {
            if(!nccs.isShuttingDown()) {
                logger.error("Received an unexpected InterruptException!", e);
            } else {
                logger.debug("Received an InterruptException and I am shutting down...", e);
            }
        }
    }

    @Synchronized
    public boolean isRunning() {
        return nccs.getConnectionState().equals(NCCState.RUNNING);
    }

    /**
     * Finalize a {@link WriteCommandBuilder} and send it out.
     * @param wcb the {@link WriteCommandBuilder} to execute
     * @return a future that will be completed when a response is available.
     */
    public NabuClientFuture executePreparedCommand(WriteCommandBuilder wcb) throws NabuClientDisconnectedException {
        synchronized (nccs) {
            return nccs.executePreparedCommand(wcb);
        }
    }

    public IndexCommandBuilder prepareIndexCommand(String indexName, String documentType) {
        return new IndexCommandBuilder(this, indexName, documentType);
    }

    public UpdateCommandBuilder prepareUpdateCommand(String indexName, String documentType) {
        return new UpdateCommandBuilder(this, indexName, documentType);
    }
}
