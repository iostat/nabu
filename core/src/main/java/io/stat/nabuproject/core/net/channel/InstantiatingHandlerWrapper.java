package io.stat.nabuproject.core.net.channel;

import io.netty.channel.ChannelHandler;

import java.lang.reflect.Constructor;

/**
 * A {@link FluentHandlerWrapper} which creates a new instance of a Class
 * every time it a handler is requested.
 *
 * The Class
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
final class InstantiatingHandlerWrapper<T extends ChannelHandler, U> implements FluentHandlerWrapper<T> {
    private final Class<T> theClass;
    private final Object[] classInitArgs;
    private final Class<?>[] classInitArgTypes;
    private final ChannelHandlerCallback<T, U> callback;
    private final U cbArgs;

    /**
     * Creates an InstantiatingHandlerWrapper that wraps a subclass of ChannelHandler.
     * If no arguments need to be passed to construct <tt>theClass</tt>, <tt>classInitArgs</tt> and <tt>classInitArgTypes</tt> can be null.
     * @param theClass the subclass to wrap
     * @param classInitArgs an array of arguments to pass to the constructor. Varargs need to be wrapped in an array at the very end.
     * @param classInitArgTypes the classes of the above arguments. must match the signature of the constructor
     * @throws IllegalArgumentException if <tt>theClass</tt> is null
     * @throws IllegalArgumentException if <tt>classInitArgs</tt> is specified, but <tt>classInitArgTypes</tt> isn't
     * @throws IllegalArgumentException if <tt>classInitArgsTypes</tt> is specified, but <tt>classInitArgs</tt> isn't
     * @throws IllegalArgumentException if the length of <tt>classInitArgs</tt> and <tt>classInitArgTypes</tt> are different.
     */
    InstantiatingHandlerWrapper(Class<T> theClass,
                                Object[] classInitArgs,
                                Class<?>[] classInitArgTypes,
                                ChannelHandlerCallback<T, U> callback,
                                U cbArgs) {

        if(theClass == null) {
            throw new IllegalArgumentException("Received null instead of a ChannelHandler class");
        }

        this.theClass = theClass;
        this.classInitArgs = classInitArgs;
        this.classInitArgTypes = classInitArgTypes;
        this.callback = callback;
        this.cbArgs = cbArgs;

        if(classInitArgs == null && classInitArgTypes == null) {
            return;
        }

        if(classInitArgs != null && classInitArgTypes == null) {
            throw new IllegalArgumentException("Constructor argument type inference is currently not supported.");
        }

        if(classInitArgs == null) {
            throw new IllegalArgumentException("Constructor argument types specified but no arguments given.");
        }

        if(classInitArgs.length != classInitArgTypes.length) {
            throw new IllegalArgumentException("Array length mismatch between constructor arguments and types thereof");
        }
    }

    
    @Override
    public T getHandler() throws Exception {
        return makeInstance();
//        if(classInitArgs == null) {
//            return theClass.newInstance();
//        } else {
//            return theClass.getDeclaredConstructor(classInitArgTypes).newInstance(classInitArgs);
//        }
    }

    private T makeInstance() throws Exception {
        boolean useNullaryConstructor = classInitArgs == null || classInitArgs.length == 0;
        Constructor<T> c;

        if(useNullaryConstructor) {
            c = theClass.getDeclaredConstructor();
        } else {
            c = theClass.getDeclaredConstructor(classInitArgTypes);
        }

        boolean wasAccessible = c.isAccessible();
        c.setAccessible(true);

        T instance;
        if(useNullaryConstructor) {
            instance = c.newInstance();
        } else {
            instance = c.newInstance(classInitArgs);
        }

        c.setAccessible(wasAccessible);
        return instance;
    }

    @Override
    public void runCallback(T instance) {
        if(callback != null) {
            callback.accept(instance, cbArgs);
        }
    }
}
