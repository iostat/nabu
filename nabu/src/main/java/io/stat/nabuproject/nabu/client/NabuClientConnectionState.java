package io.stat.nabuproject.nabu.client;

import io.netty.channel.Channel;
import io.stat.nabuproject.core.util.concurrent.ResettableCountDownLatch;
import io.stat.nabuproject.nabu.common.response.FailResponse;
import io.stat.nabuproject.nabu.common.response.NabuResponse;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.koloboke.collect.map.hash.HashLongObjMap;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;
import net.openhft.koloboke.function.LongObjConsumer;

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
    private final HashLongObjMap<NabuClientFuture> promises; // concrete type to prevent invokeinterface calls and allow hard ASM hotspot JIT
    private final NabuClient highLevelClient;
    private final AtomicReference<NabuClientIO> ncio;

    private static final LongObjConsumer<NabuClientFuture> COMPLETE_WITH_FAIL_LOC = (k, v) -> v.complete(new FailResponse(k));

    NabuClientConnectionState(NabuClient highLevelClient) {
        this.highLevelClient = highLevelClient;
        this.ncio = new AtomicReference<>(null);

        this.connectionState = new AtomicReference<>(NCCState.INITIALIZED);
        this.connectorThread = new AtomicReference<>(null);
        this.startupSynchronizer = new ResettableCountDownLatch(1);
        this.failureReason = new AtomicReference<>(null);
        this.clientChannel = new AtomicReference<>(null);
        this.promises = HashLongObjMaps.newMutableMap(2000);
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
        if(currentState != NCCState.CONNECTING) {
            throw new IllegalArgumentException("Tried to change the client connection channel outside of State.CONNECTING!");
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
            long seq = response.getSequence();
            CompletableFuture<NabuResponse> future = this.promises.getOrDefault(seq, null);
            if(future != null) {
                future.complete(response);
                this.promises.remove(seq);
            }
        }
    }

    @Override
    public void connectionLost(NabuClientIO src) {
        clearPromises();

        synchronized (this) {
            if(!(isShuttingDown() || getConnectionState() == NCCState.FAILED)) {
                setConnectionState(NCCState.DISCONNECTED);
            }
            highLevelClient.disconnect();
        }
    }

    @Override
    public void connectionInterrupted(NabuClientIO src, Throwable t) {
        synchronized(this) {
            setConnectionState(NCCState.FAILED);
            setFailureReason(new NabuConnectionFailedException("Lost connection to Nabu due to a network-related exception", t));
            clearPromises();
        }
    }

    private void clearPromises() {
        synchronized (this.promises) {
            if(!this.promises.isEmpty()) {
                promises.forEach(COMPLETE_WITH_FAIL_LOC);
            }

            this.promises.clear();
        }
    }

    @Override
    public void identificationFailed(NabuClientIO src, String expectedClusterName, String remoteClusterName) {
        synchronized (this) {
            logger.error("Nabu failed the identification test. {} != {}", expectedClusterName, remoteClusterName);
            setConnectionState(NCCState.FAILED);
            getStartupSynchronizer().countDown();
        }
    }

    @Override
    public void setClientIO(NabuClientIO ncio) {
        synchronized (this) {
            this.ncio.set(ncio);
        }
    }

    NabuClientIO getNCIO() {
        return ncio.get();
    }

    NabuClientFuture executePreparedCommand(WriteCommandBuilder wcb) throws NabuClientDisconnectedException {
        String disconnectedMessage = "The client is not connected to Nabu and thus cannot send commands.";
        if(getConnectionState() != NCCState.RUNNING) {
            throw new NabuClientDisconnectedException(disconnectedMessage);
        }

        synchronized (this) {
            if(getConnectionState() != NCCState.RUNNING) {
                // we may have gotten disconnected while we waited to lock ourselves.
                throw new NabuClientDisconnectedException("The client is not connected to Nabu and thus cannot send commands.");
            }
            NabuClientIO ncio = getNCIO();
            long sequence = ncio.assignNextSequence();

            NabuClientFuture futureForCommand = new NabuClientFuture(sequence);
            synchronized (this.promises) {
                if(getConnectionState() != NCCState.RUNNING) {
                    futureForCommand.complete(new FailResponse(sequence));
                    return futureForCommand;
                } else {
                    ncio.dispatchCommand(wcb.build(sequence));
                    promises.put(sequence, futureForCommand);
                }
            }

            return futureForCommand;
        }
    }
}
