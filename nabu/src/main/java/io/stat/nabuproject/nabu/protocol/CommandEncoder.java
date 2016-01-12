package io.stat.nabuproject.nabu.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.stat.nabuproject.core.util.ProtocolHelper;
import io.stat.nabuproject.nabu.common.NabuCommand;

/**
 * Encodes {@link NabuCommand}s that can be sent over the network and decoded by a
 * {@link CommandDecoder} on the other end.
 */
public class CommandEncoder extends MessageToByteEncoder<NabuCommand> {
    public byte[] performEncode(NabuCommand command) throws Exception {
        ByteBuf out = Unpooled.buffer();
        encode(null, command, out);
        return ProtocolHelper.convertAndRelease(out);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, NabuCommand msg, ByteBuf out) throws Exception {
        ByteBuf commandData = Unpooled.buffer();
        commandData.writeByte(msg.shouldRefresh() ? 1 : 0);
        ProtocolHelper.writeStringToByteBuf(msg.getIndex(), commandData);
        ProtocolHelper.writeStringToByteBuf(msg.getDocumentType(), commandData);

        CommandSerializers.serialize(msg, commandData);

        out.writeShort(NabuCommand.MAGIC);
        out.writeByte(msg.getType().getCode());
        out.writeInt(commandData.readableBytes());
        out.writeBytes(commandData);
    }
}
