package io.stat.nabuproject.core.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;

import java.util.List;

/**
 * Exposes Netty's {@link ObjectDecoder} to allow it to be used
 * as a "sub-decoder"
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public final class ObjectDecoderExposer extends ObjectDecoder {
    public ObjectDecoderExposer() {
        super(ClassResolvers.softCachingConcurrentResolver(null));
    }

    public void exposeDecode(ChannelHandlerContext c, ByteBuf in, List<Object> out) throws Exception {
        decode(c, in, out);
    }
}
