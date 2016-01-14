package io.stat.nabuproject.core.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.Serializable;

/**
 * Exposes Netty's {@link ObjectEncoder} to allow it to be used
 * as a "sub-encoder"
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public final class ObjectEncoderExposer extends ObjectEncoder {
    public void exposeEncode(ChannelHandlerContext c, Serializable o, ByteBuf out) throws Exception {
        encode(c, o, out);
    }
}
