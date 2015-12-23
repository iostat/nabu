package com.socialrank.nabu.protocol;

import com.socialrank.nabu.common.NabuCommand;
import com.socialrank.nabu.common.NabuCommandType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.util.List;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class NabuCommandDecoder extends ByteToMessageDecoder {
    private static class NabuBaseCommand extends NabuCommand {

        public NabuBaseCommand(String index, String documentType, boolean shouldUpdate) {
            super(index, documentType, shouldUpdate);
        }

        @Override
        public NabuCommandType getType() {
            throw new IllegalStateException("What have you done.");
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // At the very least we need at least 7 bytes to be readable
        // 2 bytes MAGIC (short)
        // 1 byte type (NabuCommandType)
        // and 4 bytes dataLength (int)
        if(in.readableBytes() < 7) {
            return;
        }

        in.markReaderIndex();

        short magic = in.readShort();
        if (magic != NabuCommand.MAGIC) {
            in.resetReaderIndex();
            throw new CorruptedFrameException("Invalid command magic. " +
                    "Received 0x" + Integer.toHexString(magic)
                    + " but expected" + NabuCommand.MAGIC_HEX_STRING);
        }

        NabuCommandType commandType = NabuCommandType.ofCode(in.readByte());
        int restOfDataLength = in.readInt();

        // wait until all the data is available
        if(in.readableBytes() < restOfDataLength) {
            in.resetReaderIndex();
            return;
        }

        boolean shouldUpdateIndex = (in.readByte() == 1);
        String indexName = ProtocolHelpers.readStringFromByteBuf(in);
        String documentType = ProtocolHelpers.readStringFromByteBuf(in);

        NabuBaseCommand base = new NabuBaseCommand(indexName, documentType, shouldUpdateIndex);
        NabuCommand fullCommand = CommandSerializers.deserialize(in, base, commandType);

        out.add(fullCommand);
    }
}
