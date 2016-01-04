package io.stat.nabuproject.core.net.channel;

import io.netty.channel.ChannelHandler;

/**
 * Something which can provide an instance of ChannelHandler
 * to initialize a SocketChannel (used by {@link FluentChannelInitializer}
 *
 * @param <T> the type of ChannelHandler that this wrapper wraps
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
interface FluentHandlerWrapper<T extends ChannelHandler> {
    /**
     * Get or create the ChannelHandler that this wrapper wraps
     * @return a ChannelHandler
     */
    T getHandler() throws Exception;

    /**
     * Run any callback that was attached to the handler wrapper.
     * @param instance the new instance to run the callback against
     */
    void runCallback(T instance);
}
