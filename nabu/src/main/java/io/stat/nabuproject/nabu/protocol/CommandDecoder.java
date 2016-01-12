package io.stat.nabuproject.nabu.protocol;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.stat.nabuproject.core.util.ProtocolHelper;
import io.stat.nabuproject.nabu.common.NabuCommand;
import io.stat.nabuproject.nabu.common.NabuCommandType;
import org.apache.kafka.common.errors.SerializationException;

import java.util.List;

/**
 * Decodes data received from the network into {@link NabuCommand}s which was encoded by
 * {@link CommandEncoder} on the other end.
 */
public class CommandDecoder extends ByteToMessageDecoder {
    private static class NabuBaseCommand extends NabuCommand {

        public NabuBaseCommand(String index, String documentType, boolean shouldUpdate) {
            super(index, documentType, shouldUpdate);
        }

        @Override
        public NabuCommandType getType() {
            throw new IllegalStateException("What have you done.");
        }
    }

    public NabuCommand performDecode(byte[] in) throws Exception {
        ByteBuf inByteBuf = Unpooled.copiedBuffer(in);
        List<Object> out = Lists.newArrayList();
        decode(null, inByteBuf, out);

        if(out.size() != 1) {
            throw new SerializationException("Received more than one object back from decode(), which should never happen");
        }

        Object first = out.get(0);

        if(!(first instanceof NabuCommand)) {
            throw new SerializationException("Received something other than a NabuCommand back from decode(), which should never happen");
        }

        return ((NabuCommand) first);
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
        String indexName = ProtocolHelper.readStringFromByteBuf(in);
        String documentType = ProtocolHelper.readStringFromByteBuf(in);

        NabuBaseCommand base = new NabuBaseCommand(indexName, documentType, shouldUpdateIndex);
        NabuCommand fullCommand = CommandSerializers.deserialize(in, base, commandType);

        out.add(fullCommand);
    }
}
