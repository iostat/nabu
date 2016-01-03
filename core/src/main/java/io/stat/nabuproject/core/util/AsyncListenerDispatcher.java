package io.stat.nabuproject.core.util;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Asynchronously dispatches arbitrary callbacks, and then runs a
 * finalizer function based on whether or not those callbacks succeeded.
 *
 * @param <T> The type of consumer that's being dispatched
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class AsyncListenerDispatcher<T> {
    private final ExecutorService workerExecutorService;
    private final ExecutorService collectorExecutorService;
    private final Set<T> listeners;

    public AsyncListenerDispatcher(ExecutorService workerExecutorService, ExecutorService collectorExecutorService) {
        this.workerExecutorService = workerExecutorService;
        this.collectorExecutorService = collectorExecutorService;

        this.listeners = Sets.newConcurrentHashSet();
    }

    /**
     * Dispatch a callback to all listeners.
     * @param callbackCallerFunction A function which calls the callback you want on ONE listener.
     * @param crc a CallbackResultsConsumer that is called when all callbacks have finished running.
     */
    public void dispatchListenerCallbacks(Consumer<T> callbackCallerFunction, CallbackResultsConsumer crc) {
        List<Future> futures = listeners.stream()
                .map(listener ->
                        workerExecutorService.submit(() -> callbackCallerFunction.accept(listener)))
                .collect(Collectors.toList());

        collectorExecutorService.submit(new CallbackResultsConsumerRunner(crc, new FutureCollectorTask(futures)));
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

    /**
     * A set of callbacks which respond to whether or not the callbacks that a AsyncListenerDispatcher
     * was told run failed and how they failed if they did.
     */
    public static abstract class CallbackResultsConsumer {

        /**
         * Called if any callback threw something.
         * @param t what a callback threw.
         */
        public abstract void failedWithThrowable(Throwable t);

        /**
         * Called if not all callbacks ran successfully.
         */
        public abstract void failed();

        /**
         * Called if all callbacks executed successfully
         */
        public abstract void success();
    }

    private class CallbackResultsConsumerRunner implements Runnable {
        private final CallbackResultsConsumer crc;
        private final FutureCollectorTask fct;

        public CallbackResultsConsumerRunner(CallbackResultsConsumer crc, FutureCollectorTask fct) {
            this.crc = crc;
            this.fct = fct;
        }

        @Override
        public void run() {
            boolean result = false;
            try {
                result = fct.call();
            } catch(Throwable t) {
                crc.failedWithThrowable(t);
            }

            if(!result) {
                crc.failed();
            } else {
                crc.success();
            }
        }
    }

    /**
     * For every future that it is assigned to run, it will see if the future failed.
     * A future's failure is determined by whether or not it threw an Exception.
     */
    @Slf4j
    public static final class FutureCollectorTask implements Callable<Boolean> {

        final List<Future> futuresToCollect;

        FutureCollectorTask(List<Future> futuresToCollect) {
            this.futuresToCollect = futuresToCollect;
        }
        @Override
        public Boolean call() throws Exception {
            for(Future f : futuresToCollect) {
                f.get();
            }
            return true;
        }

    }
}
