package io.stat.nabuproject.core.util.dispatch;

import com.google.common.collect.Sets;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.util.NamedThreadFactory;
import io.stat.nabuproject.core.util.NamedThreadPoolExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Asynchronously dispatches arbitrary callbacks, and then runs a
 * finalizer function based on whether or not those callbacks succeeded.
 *
 * A successful callback is one that returns "true".
 * A null or false implies failure.
 * An exception thrown implies an exception failure.
 *
 * @param <T> The type of consumer that's being dispatched
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class AsyncListenerDispatcher<T> extends Component {
    private final ExecutorService workerExecutorService;
    private final ExecutorService collectorExecutorService;
    private final Set<T> listeners;
    private final AtomicBoolean isShuttingDown;

    private final byte[] $executorLock;

    /**
     * Creates an AsyncListenerDispatcher with ExecutorServices generated on the fly
     * with defaults of 2 min threads, 10 max threads, 20 second timeout, a new SynchronousQueue,
     * and with {@link NamedThreadFactory}s backing them
     * @param name the name to prefix the threads with.
     */
    public AsyncListenerDispatcher(String name) {
        this(
                new NamedThreadPoolExecutor(
                        name + "-Worker",
                        2, 10,
                        20, TimeUnit.SECONDS,
                        new SynchronousQueue<>()),

                new NamedThreadPoolExecutor(
                        name + "-Collector",
                        2, 10,
                        20, TimeUnit.SECONDS,
                        new SynchronousQueue<>())
        );
    }

    /**
     * Creates an AsyncListenerDispatcher backed by the specified ExecutorServices
     * @param workerExecutorService the ExecutorService for callback workers
     * @param collectorExecutorService the ExecutorService for collecting the results of the callback workers
     */
    public AsyncListenerDispatcher(ExecutorService workerExecutorService, ExecutorService collectorExecutorService) {
        this.workerExecutorService = workerExecutorService;
        this.collectorExecutorService = collectorExecutorService;

        this.$executorLock = new byte[0];
        this.isShuttingDown = new AtomicBoolean(false);

        this.listeners = Sets.newConcurrentHashSet();
    }

    /**
     * Dispatch a callback to all listeners.
     * @param callbackCaller A function which calls the callback you want on ONE listener.
     *                       It will be applied once to each listener on a separate worker thread.
     * @param crc a CallbackReducerCallback that is called when all callbacks have finished running.
     */
    public void dispatchListenerCallbacks(Predicate<T> callbackCaller, CallbackReducerCallback crc) {
        synchronized ($executorLock) {
            if(isShuttingDown.get()) {
                logger.error("This AsyncListenerDispatcher is shutting down and cannot dispatch callbacks!\n" +
                        "Attempted to call:\n" +
                        "{}\n{}",
                        callbackCaller, crc);
                return;
            }
            List<Future<Boolean>> futures = listeners.stream()
                    .map(listener ->
                            workerExecutorService.submit(() -> callbackCaller.test(listener)))
                    .collect(Collectors.toList());

            collectorExecutorService.submit(new CallbackReducerRunner(crc, new FutureCollectorTask(crc.getName(), futures)));
        }
    }

    /**
     * Adds a listener to receive callbacks.
     * @param listener the listener to add
     */
    public void addListener(T listener) {
        listeners.add(listener);
    }

    /**
     * Removes a listener and stops it from receiving callbacks.
     * @param listener the listener to remove
     */
    public void removeListener(T listener) {
        listeners.remove(listener);
    }

    @Override
    public void shutdown() throws ComponentException {
        synchronized ($executorLock) {
            this.isShuttingDown.set(true);
            this.workerExecutorService.shutdown();
            this.collectorExecutorService.shutdown();
        }
    }
}
