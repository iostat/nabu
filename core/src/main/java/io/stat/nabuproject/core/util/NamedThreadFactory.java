package io.stat.nabuproject.core.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.experimental.Delegate;

import java.util.concurrent.ThreadFactory;

/**
 * A simple ThreadFactory which allows you to customize the name
 * of created threads. All created threads will have the format
 * <tt>[name]-N</tt> where N is the number of threads created
 * before this one.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public final class NamedThreadFactory implements ThreadFactory {
    @Getter
    @Delegate(types=ThreadFactory.class)
    private final ThreadFactory backingThreadFactory;

    /**
     * Create a new NamedThreadFactory.
     *
     * @param name The name to prefix to threads
     */
    public NamedThreadFactory(String name) {
        this.backingThreadFactory =
            new ThreadFactoryBuilder()
                    .setNameFormat(name+"-%d")
                    .build();
    }
}
