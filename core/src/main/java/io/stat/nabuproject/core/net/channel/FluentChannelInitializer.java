package io.stat.nabuproject.core.net.channel;

import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

import java.util.List;

/**
 * Allows one to create channel initializers for Netty
 * without worrying about making specific constructors/factories for
 * every kind of possible server/client socket channel handler.
 *
 * You can pass in either specific instances of ChannelHandler or a
 * class to construct (and optionally arguments).
 *
 * If you pass in an instance, that instance will be reused across all
 * channels that are initialized. If you pass in a class, one will be constructed for each
 * channel that is initialized.
 *
 * All handlers/classes instantiated will be addLast()'ed to the channel's pipeline
 * in the same order that addLast is called.
 *
 * This class is not thread-safe, although there is absolutely no reason that
 * should be a problem for anybody.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class FluentChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final List<FluentHandlerWrapper> handlers;

    /**
     * Creates a new FluentChannelInitializer
     */
    public FluentChannelInitializer() {
        this.handlers = Lists.newLinkedList();
    }

    /**
     * Adds an instance of ChannelHandler to the list of handlers that should be appended
     * to the channel pipeline when a channel is initialized. This instance will be reused
     * across all channels that this initializer initializes.
     * @param ch the channel handler to append.
     * @return this
     */
    public final FluentChannelInitializer addHandler(ChannelHandler ch) {
        return addHandler(ch, null, null);
    }

    /**
     * Similar to {@link FluentChannelInitializer#addHandler(ChannelHandler)}, except allows you
     * to specify a callback to run after the instance is added to the pipeline.
     * @param ch the ChannelHandler
     * @param cb the {@link ChannelHandlerCallback}
     * @param <T> the type of ChannelHandler this handler is
     * @return this
     */
    public final <T extends ChannelHandler> FluentChannelInitializer addHandler(T ch, ChannelHandlerCallback<T, ?> cb) {
        return addHandler(ch, cb, null);
    }

    /**
     * Similar to {@link FluentChannelInitializer#addHandler(ChannelHandler, ChannelHandlerCallback)} except
     * allows you to pass additional custom arguments to the callback.
     * @param ch the channel handler to append.
     * @param cb the callback
     * @param cbArgs any custom arguments
     * @param <T> the type of ChannelHandler this instance is
     * @param <U> the type of argumens that will be passed to the callback
     * @return this
     */
    public final <T extends ChannelHandler, U> FluentChannelInitializer addHandler(T ch,
                                                                                   ChannelHandlerCallback<T, U> cb,
                                                                                   U cbArgs) {
        handlers.add(new SimpleHandlerWrapper<>(ch, cb, cbArgs));
        return this;
    }

    /**
     * Adds a Class which will be instantiated once for each channel that this initializer
     * initializes. The constructor that will be used is the default nullary constructor.
     * @param chc the Class of ChannelHandler to instantiate for each channel that gets initialized.
     * @return this
     */
    public final <T extends ChannelHandler> FluentChannelInitializer addHandler(Class<T> chc) {
        return addHandler(chc, null);
    }

    public final <T extends ChannelHandler> FluentChannelInitializer addHandler(Class<T> chc,
                                                                                ChannelHandlerCallback<T, ?> callback) {
        return addHandler(chc, callback, null);
    }

    public final <T extends ChannelHandler, U> FluentChannelInitializer addHandler(Class<T> chc,
                                                                                   ChannelHandlerCallback<T, U> callback,
                                                                                   U args) {
        return addHandler(chc, null, null, callback, args);
    }

    public final <T extends ChannelHandler> FluentChannelInitializer addHandler(Class<T> chc, Object[] args, Class<?>[] argClasses) {
        return addHandler(chc, args, argClasses, null);
    }
    public final <T extends ChannelHandler> FluentChannelInitializer addHandler(Class<T> chc,
                                                                                Object[] args,
                                                                                Class<?>[] argClasses,
                                                                                ChannelHandlerCallback<T, ?> cb) {
        return addHandler(chc, args, argClasses, cb, null);
    }


    /**
     * Similar to {@link FluentChannelInitializer#addHandler(Class)}, except the class will
     * be constructed with a constructor that matches the types in <tt>classes</tt>, using the arguments
     * given in <tt>args</tt>. Inferring of argument types from <tt>args</tt> is not (yet) supported.
     * In the case of varags constructors, the last argument should be an array of the type expected.
     * @param chc the Class of ChannelHandler
     * @param args arguments to be passed to the constructor.
     * @param argClasses the types of arguments passed to the constructor. note these should be
     *                   qualified exactly the same way as the constructor is, (i.e., if the constructor
     *                   expects a Number argument, you can't put Integer.class as the argClass, even if
     *                   the same argument in <tt>args</tt> is an Integer.
     *
     * @return this
     */
    public final <T extends ChannelHandler, U>FluentChannelInitializer addHandler(Class<T> chc,
                                                                               Object[] args,
                                                                               Class<?>[] argClasses,
                                                                               ChannelHandlerCallback<T, U> cb,
                                                                               U cbArgs) {
        handlers.add(new InstantiatingHandlerWrapper<>(chc, args, argClasses, cb, cbArgs));
        return this;
    }

    @Override
    @SuppressWarnings("unchecked") // they're always gonna be the right type...
    protected final void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline cp = ch.pipeline();

        for(FluentHandlerWrapper wrapper : handlers) {
            ChannelHandler theHandler = wrapper.getHandler();
            wrapper.runCallback(theHandler);
            cp.addLast(theHandler);
        }
    }
}
