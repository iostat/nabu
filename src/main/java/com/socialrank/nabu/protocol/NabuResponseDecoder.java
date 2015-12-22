package com.socialrank.nabu.protocol;

import com.socialrank.nabu.common.NabuResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.util.List;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class NabuResponseDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if(in.readableBytes() < 3) {
            return;
        }

        in.markReaderIndex();

        short magic = in.readShort();
        if(magic != NabuResponse.MAGIC) {
            in.resetReaderIndex();
            throw new CorruptedFrameException("Invalid response magic. " +
                    "Received 0x" + Integer.toHexString(magic)
                    + " but expected" + NabuResponse.MAGIC_HEX_STRING);
        }

        byte respCode = in.readByte();
        try {
            NabuResponse response = NabuResponse.ofCode(respCode);
            out.add(response);
        } catch(IllegalArgumentException iae) {
            in.resetReaderIndex();
            throw new CorruptedFrameException("Cannot decode NabuResponse", iae);
        }
    }
}
