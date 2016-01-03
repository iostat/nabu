package io.stat.nabuproject.enki.server;

import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.enkiprotocol.AckOnSuccessCRC;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import io.stat.nabuproject.core.util.dispatch.AsyncListenerDispatcher;
import io.stat.nabuproject.core.util.dispatch.CallbackReducerCallback;
import io.stat.nabuproject.core.util.functional.PentaFunction;
import io.stat.nabuproject.core.util.functional.QuadFunction;
import io.stat.nabuproject.core.util.functional.TriFunction;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Dispatches callbacks to any {@link NabuConnectionListener}s registered to it asynchronously.
 */
@Slf4j
final class NabuConnectionListenerDispatcher implements NabuConnectionListener, NabuConnectionEventSource {
    @Delegate(types=Component.class)
    private final AsyncListenerDispatcher<NabuConnectionListener> dispatcher;

    NabuConnectionListenerDispatcher() {
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
        logger.info("onNewNabuConnection({})", cnxn);
        dispatchBinaryKillerTask("onNewNabuConnection", NabuConnectionListener::onNewNabuConnection, cnxn);
        return true;
    }

    @Override
    public boolean onNabuLeaving(NabuConnection cnxn, boolean serverInitiated) {
        logger.info("onNabuLeaving({}, {})", cnxn, serverInitiated);
        dispatchTernaryKillerTask("onNabuLeaving", NabuConnectionListener::onNabuLeaving, cnxn, serverInitiated);
        return true;
    }

    @Override
    public boolean onPacketDispatched(NabuConnection cnxn, EnkiPacket packet, CompletableFuture<EnkiPacket> future) {
        logger.info("onPacketDispatched({}, {}, {})", cnxn, packet, future);
        dispatchQuaternaryKillerTask(
                "onPacketDispatched",
                NabuConnectionListener::onPacketDispatched,
                cnxn, packet, future
        );
        return true;
    }

    @Override
    public boolean onNabuDisconnected(NabuConnection cnxn, boolean wasLeaving, boolean serverInitiated, boolean wasAcked) {
        logger.info("onNabuDisconnected({}, {}, {}, {}", cnxn, wasLeaving, serverInitiated, wasAcked);

        dispatchPentaryKillerTask(
                "onNabuDisconnected",
                NabuConnectionListener::onNabuDisconnected,
                cnxn,
                wasLeaving,
                serverInitiated,
                wasAcked);
        return true;
    }

    @Override
    public boolean onPacketReceived(NabuConnection cnxn, EnkiPacket packet) {
        logger.info("onPacketReceived({}, {})", cnxn, packet);
        dispatchBinaryAckerTask(
                "onPacketReceived",
                NabuConnectionListener::onPacketReceived,
                cnxn,
                packet
        );

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

    private <T extends NabuConnection, U extends EnkiPacket> void dispatchBinaryAckerTask(String callbackName,
                                                                                          TriFunction<
                                                                                                  NabuConnectionListener,
                                                                                                  T, U, Boolean> callback,
                                                                                          T cnxn, U packet) {
        dispatcher.dispatchListenerCallbacks(listener -> callback.apply(listener, cnxn, packet),
                new AckOnSuccessCRC(
                    callbackName,
                    cnxn,
                    packet));
    }

    private void dispatchKillerTask(String callbackName, NabuConnection cnxn, Function<NabuConnectionListener, Boolean> listenerConsumer) {
        dispatcher.dispatchListenerCallbacks(listenerConsumer, new KillCnxnOnFail(callbackName, cnxn));
    }

    private <T extends NabuConnection> void dispatchBinaryKillerTask(String callbackName,
                                                                     BiFunction<
                                                                             NabuConnectionListener,
                                                                             T, Boolean> callback,
                                                                     T cnxn) {
        dispatchKillerTask(callbackName, cnxn, listener -> callback.apply(listener, cnxn));
    }

    private <T extends NabuConnection, U> void dispatchTernaryKillerTask(String callbackName,
                                                                         TriFunction<
                                                                                    NabuConnectionListener,
                                                                                    T, U,
                                                                                    Boolean> callback,
                                                                     T cnxn, U arg2) {
        dispatchKillerTask(callbackName, cnxn, listener -> callback.apply(listener, cnxn, arg2));
    }

    private <T extends NabuConnection, U , V> void dispatchQuaternaryKillerTask(String callbackName,
                                                                                QuadFunction<
                                                                                        NabuConnectionListener,
                                                                                        T, U, V,
                                                                                        Boolean> callback,
                                                                                T cnxn, U arg2, V arg3) {
        dispatchKillerTask(callbackName, cnxn, listener -> callback.apply(listener, cnxn, arg2, arg3));
    }

    private <T extends NabuConnection, U, V, W> void dispatchPentaryKillerTask(String callbackName,
                                                                               PentaFunction<
                                                                                       NabuConnectionListener,
                                                                                       T, U, V, W,
                                                                                       Boolean> callback,
                                                                               T cnxn, U arg2, V arg3, W arg4) {
        dispatchKillerTask(callbackName, cnxn, listener -> callback.apply(listener, cnxn, arg2, arg3, arg4));
    }


    private static final class KillCnxnOnFail extends CallbackReducerCallback {
        private final String name;
        private final NabuConnection cnxn;

        KillCnxnOnFail(String collectionType,
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
}
