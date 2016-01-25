package io.stat.nabuproject.nabu.protocol;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.stat.nabuproject.core.net.ProtocolHelper;
import io.stat.nabuproject.nabu.common.command.IdentifyCommand;
import io.stat.nabuproject.nabu.common.command.IndexCommand;
import io.stat.nabuproject.nabu.common.command.NabuCommand;
import io.stat.nabuproject.nabu.common.command.UpdateCommand;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Decodes data received from the network into {@link NabuCommand}s which was encoded by
 * {@link CommandEncoder} on the other end.
 */
public class CommandDecoder extends ByteToMessageDecoder {
    public NabuCommand performDecode(byte[] in) throws Exception {
        ByteBuf slurped = Unpooled.wrappedBuffer(in);
        List<Object> out = Lists.newArrayList(1);
        decode(null, slurped, out);

        slurped.release();
        return ((NabuCommand)out.get(1));
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // At the very least we need at least 7 bytes to be readable
        // 2 bytes MAGIC (short)
        // 1 byte type (Type)
        // 8 bytes sequence (long)
        // and 4 bytes dataLength (int)
        if(in.readableBytes() < 15) {
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

        NabuCommand.Type commandType = NabuCommand.Type.ofCode(in.readByte());
        long sequence = in.readLong();
        int restOfDataLength = in.readInt();

        if(restOfDataLength == 0) {
            out.add(makeSimpleCommand(commandType, sequence));
            return;
        } else {
            // wait until all the data is available
            if(in.readableBytes() < restOfDataLength) {
                in.resetReaderIndex();
                return;
            }

            boolean shouldRefresh = in.readBoolean();
            boolean shouldForceWrite = in.readBoolean();
            String indexName = ProtocolHelper.readStringFromByteBuf(in);
            String documentType = ProtocolHelper.readStringFromByteBuf(in);
            String documentID = ProtocolHelper.readStringFromByteBuf(in);
            String documentSource = ProtocolHelper.readStringFromByteBuf(in);

            if(commandType == NabuCommand.Type.INDEX) {
                out.add(new IndexCommand(sequence, indexName, documentType, documentID, documentSource, shouldRefresh, shouldForceWrite));
                return;
            } else if(commandType == NabuCommand.Type.UPDATE) {
                boolean hasUpsert = in.readBoolean();
                boolean hasUpdateScript = in.readBoolean();
                String updateScript = ProtocolHelper.readStringFromByteBuf(in);
                Map<String, Serializable> scriptParams = ProtocolHelper.readSerializableFromByteBuf(in);

                out.add(new UpdateCommand(sequence, indexName, documentType, documentID, documentSource,
                        shouldRefresh, shouldForceWrite, updateScript, scriptParams, hasUpsert, hasUpdateScript));
                return;
            } else {
                throw new UnsupportedOperationException("Don't know how to decode NabuWriteCommand of type " + commandType.toString() + "!");
            }
        }

    }

    private NabuCommand makeSimpleCommand(NabuCommand.Type commandType, long sequence) throws Exception {
        if(commandType == NabuCommand.Type.IDENTIFY) {
            return new IdentifyCommand(sequence);
        } else {
            throw new UnsupportedOperationException("Don't know how to decode NabuCommand of type " + commandType.toString() + "!");
        }
    }
}
