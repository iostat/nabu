package io.stat.nabuproject.nabu.client;

import com.google.common.collect.Maps;
import io.netty.channel.Channel;
import io.stat.nabuproject.core.util.concurrent.ResettableCountDownLatch;
import io.stat.nabuproject.nabu.common.response.FailResponse;
import io.stat.nabuproject.nabu.common.response.NabuResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An internal class for keeping track of the high-level client's connection state.
 */
@Slf4j
final class NabuClientConnectionState implements HighLevelNabuClientBridge {
    private final AtomicReference<NCCState> connectionState;
    private final AtomicReference<Thread> connectorThread;
    private final @Getter ResettableCountDownLatch startupSynchronizer;
    private final AtomicReference<NabuConnectionFailedException> failureReason;
    private final AtomicReference<Channel> clientChannel;
    private final Map<Long, CompletableFuture<NabuResponse>> promises;
    private final NabuClient highLevelClient;


    NabuClientConnectionState(NabuClient highLevelClient) {
        this.highLevelClient = highLevelClient;

        this.connectionState = new AtomicReference<NCCState>(NCCState.INITIALIZED);
        this.connectorThread = new AtomicReference<>(null);
        this.startupSynchronizer = new ResettableCountDownLatch(1);
        this.failureReason = new AtomicReference<>(null);
        this.clientChannel = new AtomicReference<>(null);
        this.promises = Maps.newHashMap();
    }

    Thread getConnectorThread() {
        return connectorThread.get();
    }

    void setConnectorThread(Thread newValue) {
        connectorThread.set(newValue);
    }

    boolean isShuttingDown() {
        return getConnectionState() == NCCState.SHUTTING_DOWN;
    }

    void isShuttingDown(boolean wellIsIt) {
        if (wellIsIt && getConnectionState() != NCCState.SHUTTING_DOWN) {
            setConnectionState(NCCState.SHUTTING_DOWN);
        }
    }

    NabuConnectionFailedException getFailureReason() {
        return failureReason.get();
    }

    void setFailureReason(NabuConnectionFailedException reason) {
        if(failureReason.get() == null) {
            failureReason.set(reason);
        } else {
            throw new IllegalArgumentException("Cannot re-set the failure reason!");
        }
    }

    NCCState getConnectionState() {
        return connectionState.get();
    }

    void setConnectionState(NCCState newState) {
        if(connectionState.get() == NCCState.SHUTTING_DOWN && !(newState == NCCState.DISCONNECTED || newState == NCCState.FAILED)) {
            throw new IllegalArgumentException("Tried to change the Nabu client state to " + newState.toString() + " rather than DISCONNECTED or FAILED after it was told to shut down");
        } else {
            connectionState.set(newState);
        }
    }

    Channel getClientChannel() {
        return clientChannel.get();
    }

    void setClientChannel(Channel newChannel) {
        NCCState currentState = connectionState.get();
        if(currentState != NCCState.CONNECTING && currentState != NCCState.RETRYING) {
            throw new IllegalArgumentException("Tried to change the client connection channel outside of an allowable state to do so");
        }

        clientChannel.set(newChannel);
    }

    @Override
    public void connectionEstablished(NabuClientIO src) {
        synchronized (this) {
            setConnectionState(NCCState.RUNNING);
            getStartupSynchronizer().countDown();
        }
    }

    @Override
    public void responseReceived(NabuClientIO src, NabuResponse response) {
        synchronized(this.promises) {
            CompletableFuture<NabuResponse> future = this.promises.getOrDefault(response.getSequence(), null);
            if(future != null) {
                future.complete(response);
            }
        }
    }

    @Override
    public void connectionLost(NabuClientIO src) {
        synchronized (this.promises) {
            if(!this.promises.isEmpty()) {
                promises.forEach((k, v) -> v.complete(new FailResponse(k)));
                this.promises.clear();
            }
        }

        synchronized (this) {
            setConnectionState(NCCState.DISCONNECTED);
        }

        highLevelClient.disconnect();
    }

    @Override
    public void identificationFailed(NabuClientIO src, String expectedClusterName, String remoteClusterName) {
        synchronized (this) {
            logger.error("Nabu failed the identification test. {} != {}", expectedClusterName, remoteClusterName);
            setConnectionState(NCCState.RETRYING);
            getStartupSynchronizer().countDown();
        }
    }
}
