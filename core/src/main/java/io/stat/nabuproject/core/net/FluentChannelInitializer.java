package io.stat.nabuproject.core.net;

import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

import java.util.List;

/**
 * Allows one to create channel initializers for Netty
 * without worrying about making specific constructors for every kind of
 * possible server/client socket channel handler.
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
     */
    public final void addHandler(ChannelHandler ch) {
        handlers.add(new FluentHandlerWrapper(ch));
    }

    /**
     * Adds a Class which will be instantiated once for each channel that this initializer
     * initializes. The constructor that will be used is the default nullary constructor.
     * @param chc the Class of ChannelHandler to instantiate for each channel that gets initialized.
     */
    public final void addHandler(Class<? extends ChannelHandler> chc) {
        addHandler(chc, null, null);
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
     */
    public final void addHandler(Class<? extends ChannelHandler> chc, Object[] args, Class<?>[] argClasses) {
        handlers.add(new FluentHandlerWrapper(chc, args, argClasses));
    }

    @Override
    protected final void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline cp = ch.pipeline();

        for(FluentHandlerWrapper wrapper : handlers) {
            cp.addLast(wrapper.getHandler());
        }
    }

    /**
     * Wraps either a subclass or instance of ChannelHandler.
     */
    private static final class FluentHandlerWrapper {
        private final ChannelHandler instance;
        private final Class<? extends ChannelHandler> theClass;
        private final Object[] classInitArgs;
        private final Class<?>[] classInitArgTypes;

        /**
         * Creates a FluentHandlerWrapper that wraps a concrete instance
         * of ChannelHandler
         * @param instance the instance to wrap.
         */
        FluentHandlerWrapper(ChannelHandler instance) {
            this.instance = instance;
            this.theClass = null;
            this.classInitArgs = null;
            this.classInitArgTypes = null;
        }

        /**
         * Creates a FluentHandlerWrapper that wraps a sub-Class of
         * ChannelHandler. If no arguments need to be passed to construct <tt>theClass</tt>,
         * <tt>classInitArgs</tt> and <tt>classInitArgTypes</tt> can be null.
         * @param theClass the subclass to wrap
         * @param classInitArgs an array of arguments to pass to the constructor. Varargs need to be wrapped in an array at the very end.
         * @param classInitArgTypes the classes of the above arguments. must match the signature of the constructor
         * @throws IllegalArgumentException if <tt>classInitArgs</tt> is specified, but <tt>classInitArgTypes</tt> isnt
         * @throws IllegalArgumentException if the length of <tt>classInitArgs</tt> and <tt>classInitArgTypes</tt> are different.
         */
        FluentHandlerWrapper(Class<? extends ChannelHandler> theClass,
                             Object[] classInitArgs,
                             Class<?>[] classInitArgTypes) {
            this.instance = null;

            this.theClass = theClass;
            this.classInitArgs = classInitArgs;
            this.classInitArgTypes = classInitArgTypes;

            if(classInitArgs == null && classInitArgTypes == null) {
                return;
            }

            if(classInitArgs != null && classInitArgTypes == null) {
                throw new IllegalArgumentException("Constructor argument type inference is currently not supported.");
            }

            if(classInitArgs.length != classInitArgTypes.length) {
                throw new IllegalArgumentException("Array length mismatch between constructor arguments and types thereof");
            }
        }

        /**
         * Gets the handler that this wrapper wraps. If this class was constructed with an
         * instance of ChannelHandler, it returns that instance. If not, it will attempt to
         * construct a new class and return that.
         * @return an instance of ChannelHandler
         * @throws Exception if any exceptions get thrown when constructing an instance.
         */
        ChannelHandler getHandler() throws Exception {
            if(instance != null) {
                return instance;
            } else {
                if(this.classInitArgs == null) {
                    return theClass.newInstance();
                } else {
                    return theClass.getDeclaredConstructor(classInitArgTypes).newInstance(classInitArgs);
                }
            }
        }
    }
}
