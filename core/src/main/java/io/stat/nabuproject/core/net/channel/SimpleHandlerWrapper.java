package io.stat.nabuproject.core.net.channel;

import io.netty.channel.ChannelHandler;

/**
 * Simply wraps an instance of a ChannelHandler, and returns that.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
final class SimpleHandlerWrapper<T extends ChannelHandler, U> implements FluentHandlerWrapper<T> {
    private final T theInstance;
    private final ChannelHandlerCallback<T, U> callback;
    private final U cbArgs;

    SimpleHandlerWrapper(T theInstance, ChannelHandlerCallback<T, U> callback, U args) {
        this.theInstance = theInstance;
        this.callback = callback;
        this.cbArgs   = args;
    }

    @Override
    public T getHandler() throws Exception {
        return theInstance;
    }

    @Override
    public void runCallback(T instance) {
        if(callback != null) {
            callback.accept(instance, cbArgs);
        }
    }
}
