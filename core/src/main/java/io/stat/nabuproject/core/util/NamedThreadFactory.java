package io.stat.nabuproject.core.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple ThreadFactory which allows you to customize the name
 * of created threads. All created threads will have the format
 * <tt>[name]-N</tt> where N is the number of threads created
 * before this one. Additionally, they will all be added to a ThreadGroup
 * named "name", to make debugging inhuman quantities of threads easier.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public final class NamedThreadFactory implements ThreadFactory {
    private final String nameFormat;
    private final ThreadGroup tg;
    private final AtomicLong count;

    /**
     * Create a new NamedThreadFactory.
     *
     * @param name The name to prefix to threads
     */
    public NamedThreadFactory(String name) {;
        this.nameFormat = name+"-%d";
        this.tg = new ThreadGroup(name);
        this.count = new AtomicLong(0);
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(tg, r, String.format(nameFormat, count.getAndIncrement()));
    }

    public Thread newThread(Runnable r, String overrideName) {
        return new Thread(tg, r, overrideName);
    }

    public ThreadFactory buildGroupedTFWithConstantName(String name) {
        return (runnable) ->
                this.newThread(runnable, name);
    }
}
