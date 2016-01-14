package io.stat.nabuproject.core.util;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;

import java.util.List;
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
    private final @Getter ThreadGroup threadGroup;
    private final AtomicLong count;

    /**
     * Create a new NamedThreadFactory.
     *
     * @param name The name to prefix to threads
     */
    public NamedThreadFactory(String name) {
        this.nameFormat = name+"-%d";
        this.threadGroup = new ThreadGroup(name);
        this.count = new AtomicLong(0);
    }

    public NamedThreadFactory(String name, ThreadGroup parent) {
        this.nameFormat = name+"%d";
        this.threadGroup = new ThreadGroup(parent, name);
        this.count = new AtomicLong(0);
    }

    @Override
    public synchronized Thread newThread(Runnable r) {
        return new Thread(threadGroup, r, String.format(nameFormat, count.getAndIncrement()));
    }

    public synchronized Thread newThread(Runnable r, String overrideName) {
        return new Thread(threadGroup, r, overrideName);
    }

    public synchronized ThreadFactory buildGroupedTFWithConstantName(String name) {
        return (runnable) ->
                this.newThread(runnable, name);
    }

    public synchronized NamedThreadFactory childFactory(String name) {
        return new NamedThreadFactory(name, getThreadGroup());
    }

    /**
     * <code>interrupt()</code>s and attempts to <code>join()</code> before finally <code>stop()</code>ping
     * all the Threads and sub groups that this factory is directly or indirectly responsible for.
     * @param perThreadTimeout how long to attempt to join on each thread in this factory.
     * @throws ThreadNukeException if some threads were stubborn and could not be stopped.
     */
    public synchronized void nukeCreatedHeirarchy(long perThreadTimeout) throws ThreadNukeException {
        Thread[] threads = new Thread[getThreadGroup().activeCount()];
        getThreadGroup().enumerate(threads, true);

        ImmutableList.Builder<ThreadNukeFailure> failures = ImmutableList.builder();

        for(Thread t : threads) {
            if(!t.isAlive()) {
                continue;
            }

            try {
                t.interrupt();
                t.join(perThreadTimeout);
                if(t.isAlive()) {
                    throw new InterruptedException("dummy");
                }
            } catch (Exception e1) {
                if(e1 instanceof InterruptedException && e1.getMessage().equals("dummy")) {
                    t.stop();
                    try {
                        t.join(perThreadTimeout);
                        if(t.isAlive()) {
                            failures.add(new ThreadNukeFailure(t, new Exception("Thread is basically the living dead.")));
                        }
                    } catch (Exception e2) {
                        failures.add(new ThreadNukeFailure(t, e2));
                    }
                } else {
                    failures.add(new ThreadNukeFailure(t, e1));
                }
            }
        }

        List<ThreadNukeFailure> failuresBuilt = failures.build();
        if(failuresBuilt.size() != 0) {
            throw new ThreadNukeException(failuresBuilt);
        }
    }

    @EqualsAndHashCode(callSuper = true) @ToString
    public static final class ThreadNukeException extends Exception {
        private final @Getter List<ThreadNukeFailure> failures;

        ThreadNukeException(List<ThreadNukeFailure> failures) {
            super("One or more threads failed to be nuked.");
            this.failures = failures;
        }
    }

    @Value
    public static final class ThreadNukeFailure {
        Thread thread;
        Throwable reason;
    }
}
