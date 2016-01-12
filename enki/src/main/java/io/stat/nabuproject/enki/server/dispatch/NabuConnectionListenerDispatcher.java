package io.stat.nabuproject.enki.server.dispatch;

import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.dispatch.AckOnSuccessCRC;
import io.stat.nabuproject.core.enkiprotocol.dispatch.KillCnxnOnFailCRC;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import io.stat.nabuproject.core.util.dispatch.AsyncListenerDispatcher;
import io.stat.nabuproject.enki.server.NabuConnection;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Dispatches callbacks to any {@link NabuConnectionListener}s registered to it asynchronously.
 */
@Slf4j
public final class NabuConnectionListenerDispatcher implements NabuConnectionListener, NabuConnectionEventSource {
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
    public boolean onNewNabuConnection(NabuConnection cnxn) {
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onNewNabuConnection(cnxn),
                new KillCnxnOnFailCRC("onNewNabuConnection", cnxn));
        return true;
    }

    @Override
    public boolean onNabuLeaving(NabuConnection cnxn, boolean serverInitiated) {
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onNabuLeaving(cnxn, serverInitiated),
                new KillCnxnOnFailCRC("onNabuLeaving", cnxn));

        return true;
    }

    @Override
    public boolean onPacketDispatched(NabuConnection cnxn, EnkiPacket packet, CompletableFuture<EnkiPacket> future) {
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onPacketDispatched(cnxn, packet, future),
                new KillCnxnOnFailCRC("onPacketDispatched", cnxn));
        return true;
    }

    @Override
    public boolean onNabuDisconnected(NabuConnection cnxn, EnkiConnection.DisconnectCause cause, boolean wasAcked) {
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onNabuDisconnected(cnxn, cause, wasAcked),
                new KillCnxnOnFailCRC("onNabuDisconnected", cnxn));
        return true;
    }

    @Override
    public boolean onPacketReceived(NabuConnection cnxn, EnkiPacket packet) {
        dispatcher.dispatchListenerCallbacks(listener -> listener.onPacketReceived(cnxn, packet),
                new AckOnSuccessCRC(
                        "onPacketReceived",
                        cnxn,
                        packet));
        return true;
    }

    @Override
    public boolean onNabuReady(NabuConnection cnxn) {
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onNabuReady(cnxn),
                new KillCnxnOnFailCRC("onNabuDisconnected", cnxn));
        return true;
    }

    @Override
    public void addNabuConnectionListener(NabuConnectionListener listener) {
        dispatcher.addListener(listener);
    }

    @Override
    public void removeNabuConnectionListener(NabuConnectionListener listener) {
        dispatcher.removeListener(listener);
    }
}
