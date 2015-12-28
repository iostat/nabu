package io.stat.nabuproject.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.stat.nabuproject.common.NabuResponse;

import java.util.List;

/**
 * Decodes data which was encoded by a {@link ResponseEncoder} into a {@link NabuResponse}
 */
public class ResponseDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // a response is 3 bytes,
        // 2 bytes MAGIC (short)
        // and 1 byte response code (NabuResponse)
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
