package io.stat.nabuproject.core.net.channel;

import io.netty.channel.ChannelHandler;

import java.util.function.BiConsumer;

/**
 * A callback that may be provided to {@link FluentChannelInitializer}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@FunctionalInterface
public interface ChannelHandlerCallback<T extends ChannelHandler, U> extends BiConsumer<T, U> {
    /**
     * Run the callback
     * @param theHandler the ChannelHandler that was created
     * @param customArgs any custom args that were passed. may be null
     */
    void accept(T theHandler, U customArgs);
}
