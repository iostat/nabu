package io.stat.nabuproject.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.stat.nabuproject.common.NabuResponse;

/**
 * Encodes {@link NabuResponse}s into data which can be decoded by a {@link ResponseDecoder} on
 * the other side.
 */
public class ResponseEncoder extends MessageToByteEncoder<NabuResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, NabuResponse msg, ByteBuf out) throws Exception {
        out.writeShort(NabuResponse.MAGIC);
        out.writeByte(msg.getCode());
    }
}
