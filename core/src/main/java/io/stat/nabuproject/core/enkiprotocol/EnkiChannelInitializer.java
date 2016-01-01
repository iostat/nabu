package io.stat.nabuproject.core.enkiprotocol;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;

import java.util.stream.Stream;

/**
 * A Netty ChannelInitializer that speaks in EnkiPackets
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final Object[] allHandlers;

    public EnkiChannelInitializer() {
        this(new ChannelHandler[0]);
    }

    /**
     * Creates an EnkiChannelInitializer. It adds an
     * {@link io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket.Encoder}
     * and an
     * {@link io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket.Decoder}
     * to the very start of the channel pipeline for the socket channel and
     * anything in <tt>extraHandlers</tt> afterwards, in the order they appear.
     *
     * <tt>extraHandlers</tt> should be an array of any combination of ChannelHandler,
     * or Class&lt;? extends ChannelHandler&gt;.
     *
     * For every element in the array, it will either add the instance that was given if the element was
     * a ChannelHandler to the pipeline, or if it's a Class, it will create a new instance of that class.
     * The class must have an empty constructor.
     *
     * @param extraHandlers any handlers you want other than the Packet. See description for limitations.
     * @throws IllegalArgumentException if any handler does not fit the above description
     * @throws IllegalArgumentException if the class passed in doesn't have an empty constructor
     * @throws NullPointerException if any element passed in is null
     */
    public EnkiChannelInitializer(Object... extraHandlers) {
        allHandlers = extraHandlers;

        for(int extraHandlersIdx = 0; extraHandlersIdx < extraHandlers.length; extraHandlersIdx++) {
            Object o = extraHandlers[extraHandlersIdx];
            if(o instanceof ChannelHandler) {
                continue;
            } else if(o instanceof Class) {
                Class handlerClass = ((Class)o);
                if(ChannelHandler.class.isAssignableFrom(handlerClass)) {
                    boolean hasDefaultConstructor =
                            Stream.of(handlerClass.getConstructors())
                                  .anyMatch(c -> c.getParameterCount() == 0);
                    if(hasDefaultConstructor) {
                        continue;
                    } else {
                        throw new IllegalArgumentException(String.format(
                                "Class<%s> at index %d does not have a parameterless constructor",
                                handlerClass.getCanonicalName(),
                                extraHandlersIdx));
                    }
                } else {
                    throw new IllegalArgumentException(String.format(
                            "Class<%s> at index %d is not a ChannelHandler",
                            handlerClass.getCanonicalName(),
                            extraHandlersIdx));
                }
            } else if (o == null) {
                throw new NullPointerException(String.format(
                        "Object at index %d is null",
                        extraHandlersIdx));
            } else {
                throw new IllegalArgumentException(String.format(
                        "Object of type %s at index %d is not a ChannelHandler nor a Class thereof",
                        o.getClass().getCanonicalName(),
                        extraHandlersIdx));
            }
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline cp = ch.pipeline();

        cp.addLast(new EnkiPacket.Encoder(), new EnkiPacket.Decoder());

        for(Object o : allHandlers) {
            ChannelHandler handler;
            if(o instanceof Class)
                handler = ((ChannelHandler) ((Class) o).newInstance());
            else
                handler = ((ChannelHandler) o);

            cp.addLast(handler);
        }
    }
}
