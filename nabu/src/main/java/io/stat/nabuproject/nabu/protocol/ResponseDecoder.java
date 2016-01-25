package io.stat.nabuproject.nabu.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import io.stat.nabuproject.core.net.ProtocolHelper;
import io.stat.nabuproject.nabu.common.response.FailResponse;
import io.stat.nabuproject.nabu.common.response.IDResponse;
import io.stat.nabuproject.nabu.common.response.NabuResponse;
import io.stat.nabuproject.nabu.common.response.OKResponse;
import io.stat.nabuproject.nabu.common.response.QueuedResponse;
import io.stat.nabuproject.nabu.common.response.RetryResponse;

import java.util.List;

/**
 * Decodes data which was encoded by a {@link ResponseEncoder} into a {@link NabuResponse}
 */
public class ResponseDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // a response is 3 bytes,
        // 2 bytes MAGIC (short)
        // 1 byte response code (Type)
        // and 8 bytes sequence. (long)
        if(in.readableBytes() < 11) {
            return;
        }

        in.markReaderIndex();

        short magic = in.readShort();
        if(magic != NabuResponse.MAGIC) {
            in.resetReaderIndex();
            throw new CorruptedFrameException("Invalid response magic. " +
                    "Received 0x" + Integer.toHexString(magic)
                    + " but expected " + NabuResponse.MAGIC_HEX_STRING);
        }

        byte respCode = in.readByte();
        long sequence = in.readLong();

        try {
            NabuResponse.Type type = NabuResponse.Type.ofCode(respCode);
            String id;
            try {
                id = ProtocolHelper.readStringFromByteBuf(in);
            } catch(DecoderException de) {
                if(de.getMessage().equals(ProtocolHelper.READ_STRING_NO_SIZE)
                        || de.getMessage().equals(ProtocolHelper.READ_STRING_TOO_SHORT)) {
                    // thrown inside ProtocolHelper when there's not enough data to read the String.
                    in.resetReaderIndex();
                    return;
                } else {
                    throw de;
                }
            }

            if(type == NabuResponse.Type.FAIL) {
                try {
                    String reason = ProtocolHelper.readStringFromByteBuf(in);
                    out.add(new FailResponse(sequence, id, reason));
                    return;
                } catch(DecoderException de) {
                    if(de.getMessage().equals(ProtocolHelper.READ_STRING_NO_SIZE)
                            || de.getMessage().equals(ProtocolHelper.READ_STRING_TOO_SHORT)) {
                        // thrown inside ProtocolHelper when there's not enough data to read the String.
                        in.resetReaderIndex();
                        return;
                    } else {
                        throw de;
                    }
                }
            } else {
                switch(type) {
                    case OK:
                        out.add(new OKResponse(sequence, id));
                        return;
                    case QUEUED:
                        out.add(new QueuedResponse(sequence, id));
                        return;
                    case RETRY:
                        out.add(new RetryResponse(sequence, id));
                        return;
                    case ID:
                        out.add(new IDResponse(sequence, id));
                        return;
                    default:
                        throw new IllegalArgumentException("Cannot handle NabuResponse of type " + type.toString());
                }
            }
        } catch(IllegalArgumentException iae) {
            in.resetReaderIndex();
            throw new CorruptedFrameException("Cannot decode NabuResponse", iae);
        }
    }
}
