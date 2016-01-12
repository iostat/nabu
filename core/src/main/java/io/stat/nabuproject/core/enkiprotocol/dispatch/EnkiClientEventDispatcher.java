package io.stat.nabuproject.core.enkiprotocol.dispatch;

import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiClient;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAssign;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiConfigure;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiRedirect;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiUnassign;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import io.stat.nabuproject.core.util.dispatch.AsyncListenerDispatcher;
import io.stat.nabuproject.core.util.dispatch.CallbackReducerCallback;
import io.stat.nabuproject.core.util.dispatch.ShutdownOnFailureCRC;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * An adapter for dispatching {@link EnkiClientEventListener} events asynchronously.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public final class EnkiClientEventDispatcher extends Component implements EnkiClientEventListener, EnkiClientEventSource {
    @Delegate(types=Component.class, excludes=Component.Undelegatable.class)
    private final AsyncListenerDispatcher<EnkiClientEventListener> dispatcher;
    private final CallbackReducerCallback CALLBACK_FAILED_SHUTDOWNER;

    public EnkiClientEventDispatcher(EnkiClient enkiClient) {
        // todo: figure out optimal thread pool sized because
        // honestly these thread pool sizes are beyond overkill
        // ditto for the timeouts
        // especially for the collector
        // (it has to be larger than the worker timeout
        //  though so that the workers it's collecting against
        //  can timeout without causing the collector to timeout)

        // todo: make these configurable?
        // 5 min ThreadPool size
        // 120 max threads
        // 60 second timeout
        // backed by a SynchronousQueue.
        // with thread names starting with NCLDWorker
        ExecutorService dispatchWorkerExecutor = new ThreadPoolExecutor(
                5, 120,
                60, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new NamedThreadFactory("EnkiClientEventDispatcher-Worker")
        );

        // 60 fixed pool size.
        // 5 minute timeout
        // with thread names starting with NCLDCollector
        ExecutorService collectorWorkerExecutor = new ThreadPoolExecutor(
                60, 60,
                5, TimeUnit.MINUTES,
                new SynchronousQueue<>(),
                new NamedThreadFactory("EnkiClientEventDispatcher-Collector")
        );

        this.CALLBACK_FAILED_SHUTDOWNER = new ShutdownOnFailureCRC(enkiClient, "EnkiClientEventDispatcherFailShutdowner");
        this.dispatcher = new AsyncListenerDispatcher<>(dispatchWorkerExecutor, collectorWorkerExecutor);
    }

    @Override
    public boolean onRedirected(EnkiConnection enki, long sequence, AddressPort redirectedTo) {
        logger.info("onRedirected({}, {})", enki, redirectedTo);
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onRedirected(enki, sequence, redirectedTo),
                new AckOnSuccessCRC("onRedirected", enki, new EnkiRedirect(sequence, redirectedTo)));
        return true;

    }

    @Override
    public boolean onConfigurationReceived(EnkiConnection enki, EnkiConfigure packet) {
        logger.info("onConfigurationReceived({}, {})", enki, packet);
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onConfigurationReceived(enki, packet),
                new AckOrDieCRC("onConfigurationReceived", enki, packet));
        return true;
    }

    @Override
    public boolean onTaskAssigned(EnkiConnection enki, EnkiAssign packet) {
        logger.info("onTaskAssigned({}, {})", enki, packet);
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onTaskAssigned(enki, packet),
                new AckOrDieCRC("onTaskAssigned", enki, packet));
        return true;
    }

    @Override
    public boolean onTaskUnassigned(EnkiConnection enki, EnkiUnassign packet) {
        logger.info("onTaskUnassigned({}, {})", enki, packet);
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onTaskUnassigned(enki, packet),
                new AckOrDieCRC("onTaskUnassigned", enki, packet));
        return true;
    }

    @Override
    public boolean onConnectionLost(EnkiConnection enki, EnkiConnection.DisconnectCause cause, boolean wasAcked) {
        logger.info("onConnectionLost({}, {}, {})", enki, cause, wasAcked);
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onConnectionLost(enki, cause, wasAcked),
                CALLBACK_FAILED_SHUTDOWNER);
        return true;
    }

    @Override
    public void addEnkiClientEventListener(EnkiClientEventListener ecel) {
        logger.info("Registered ECEL :: {}", ecel);
        dispatcher.addListener(ecel);
    }

    @Override
    public void removeEnkiClientEventListener(EnkiClientEventListener ecel) {
        logger.info("Deregistered ECEL :: {}", ecel);
        dispatcher.removeListener(ecel);
    }
}
