package io.stat.nabuproject.core.util.concurrent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by io on 1/12/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class NamedThreadPoolExecutor extends ThreadPoolExecutor {
    public static final int MIN_SIZE = 120;
    public static final int MAX_SIZE = 120;
    public static final long TIMEOUT  = 60;
    public static final TimeUnit TIMEOUT_UNIT = TimeUnit.SECONDS;


    public NamedThreadPoolExecutor(String name) {
        super(
                MIN_SIZE, MAX_SIZE,
                TIMEOUT, TIMEOUT_UNIT,
                new SynchronousQueue<>(),
                new NamedThreadFactory(name)
        );
    }

    public NamedThreadPoolExecutor(String name,
                                   int min, int max,
                                   long timeout, TimeUnit timeoutUnit,
                                   BlockingQueue<Runnable> queue) {
        super(
                min, max, timeout, timeoutUnit, queue, new NamedThreadFactory(name)
        );
    }
}
