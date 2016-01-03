package io.stat.nabuproject.core.util.dispatch;

import com.google.common.collect.Sets;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
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

    public AsyncListenerDispatcher(ExecutorService workerExecutorService, ExecutorService collectorExecutorService) {
        this.workerExecutorService = workerExecutorService;
        this.collectorExecutorService = collectorExecutorService;

        this.listeners = Sets.newConcurrentHashSet();
    }

    /**
     * Dispatch a callback to all listeners.
     * @param callbackCaller A function which calls the callback you want on ONE listener.
     *                       It will be applied once to each listener on a separate worker thread.
     * @param crc a CallbackReducerCallback that is called when all callbacks have finished running.
     */
    public void dispatchListenerCallbacks(Function<T, Boolean> callbackCaller, CallbackReducerCallback crc) {
        List<Future<Boolean>> futures = listeners.stream()
                .map(listener ->
                        workerExecutorService.submit(() -> callbackCaller.apply(listener)))
                .collect(Collectors.toList());

        collectorExecutorService.submit(new CallbackReducerRunner(crc, new FutureCollectorTask(futures)));
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
        this.workerExecutorService.shutdown();
        this.collectorExecutorService.shutdown();
        super.shutdown();
    }

    private class CallbackReducerRunner implements Runnable {
        private final CallbackReducerCallback cr;
        private final FutureCollectorTask fct;

        public CallbackReducerRunner(CallbackReducerCallback crc, FutureCollectorTask fct) {
            this.cr = crc;
            this.fct = fct;
        }

        @Override
        public void run() {
            boolean result = false;
            try {
                result = fct.call();
            } catch(Throwable t) {
                cr.failedWithThrowable(t);
            }

            if(!result) {
                cr.failed();
            } else {
                cr.success();
            }
        }
    }

    /**
     * For every future that it is assigned to run, it will see if the future failed.
     * A future's failure is determined by whether or not it threw an Exception, or if it
     * returned null (kind of impossible) or false. In the case of the former failure case, it
     * is called an "exceptional failure"
     */
    @Slf4j
    public static final class FutureCollectorTask implements Callable<Boolean> {
        final List<Future<Boolean>> futuresToCollect;

        FutureCollectorTask(List<Future<Boolean>> futuresToCollect) {
            this.futuresToCollect = futuresToCollect;
        }
        @Override
        public Boolean call() throws Exception {
            for(Future<Boolean> f : futuresToCollect) {
                Boolean thisResult = f.get();

                if(thisResult == null || !thisResult) {
                    return false;
                }
            }
            return true;
        }
    }
}
