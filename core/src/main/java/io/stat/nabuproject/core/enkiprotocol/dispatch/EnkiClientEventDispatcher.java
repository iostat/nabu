package io.stat.nabuproject.core.enkiprotocol.dispatch;

import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiClient;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiRedirect;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import io.stat.nabuproject.core.util.dispatch.AsyncListenerDispatcher;
import io.stat.nabuproject.core.util.dispatch.CallbackReducerCallback;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Map;
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
        // 30 max threads
        // 60 second timeout
        // backed by a SynchronousQueue.
        // with thread names starting with NCLDWorker
        ExecutorService dispatchWorkerExecutor = new ThreadPoolExecutor(
                5, 30,
                60, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new NamedThreadFactory("EnkiClientEventDispatcher-Worker")
        );

        // 20 fixed pool size.
        // 5 minute timeout
        // with thread names starting with NCLDCollector
        ExecutorService collectorWorkerExecutor = new ThreadPoolExecutor(
                20, 20,
                5, TimeUnit.MINUTES,
                new SynchronousQueue<>(),
                new NamedThreadFactory("EnkiClientEventDispatcher-Collector")
        );

        this.CALLBACK_FAILED_SHUTDOWNER = new CallbackReducerCallback() {
            @Override
            public void failedWithThrowable(Throwable t) {
                logger.error("Shutting down because some callback(s) failed with: ", t);
                enkiClient.shutdown();
            }

            @Override
            public void failed() {
                logger.error("Callback(s) failed with no throwable thrown. Shutting down.");
                enkiClient.shutdown();
            }

            @Override
            public void success() {/* no-op */ }
        };
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
    public boolean onConfigurationReceived(EnkiConnection enki, Map<String, Serializable> config) {
        logger.info("onConfigurationReceived({}, {})", enki, config);
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onConfigurationReceived(enki, config),
                CALLBACK_FAILED_SHUTDOWNER);
        return true;
    }

    @Override
    public boolean onTaskAssigned(EnkiConnection enki, TopicPartition topicPartition) {
        logger.info("onTaskAssigned({}, {})", enki, topicPartition);
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onTaskAssigned(enki, topicPartition),
                CALLBACK_FAILED_SHUTDOWNER);
        return true;
    }

    @Override
    public boolean onTaskUnassigned(EnkiConnection enki, TopicPartition topicPartition) {
        logger.info("onUnTaskAssigned({}, {})", enki, topicPartition);
        dispatcher.dispatchListenerCallbacks(
                listener -> listener.onTaskUnassigned(enki, topicPartition),
                CALLBACK_FAILED_SHUTDOWNER);
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
        dispatcher.addListener(ecel);
    }

    @Override
    public void removeEnkiClientEventListener(EnkiClientEventListener ecel) {
        dispatcher.removeListener(ecel);
    }
}
