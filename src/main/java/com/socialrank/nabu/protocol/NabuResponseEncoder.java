package com.socialrank.nabu.protocol;

import com.socialrank.nabu.common.NabuResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class NabuResponseEncoder extends MessageToByteEncoder<NabuResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, NabuResponse msg, ByteBuf out) throws Exception {
        out.writeShort(NabuResponse.MAGIC);
        out.writeByte(msg.getCode());
    }
}
