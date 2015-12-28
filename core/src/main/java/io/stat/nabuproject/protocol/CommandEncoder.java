package io.stat.nabuproject.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.stat.nabuproject.common.NabuCommand;

/**
 * Encodes {@link NabuCommand}s that can be sent over the network and decoded by a
 * {@link CommandDecoder} on the other end.
 */
public class CommandEncoder extends MessageToByteEncoder<NabuCommand> {
    @Override
    protected void encode(ChannelHandlerContext ctx, NabuCommand msg, ByteBuf out) throws Exception {
        ByteBuf commandData = Unpooled.buffer();
        commandData.writeByte(msg.shouldUpdateIndex() ? 1 : 0);
        ProtocolHelpers.writeStringToByteBuf(msg.getIndex(), commandData);
        ProtocolHelpers.writeStringToByteBuf(msg.getDocumentType(), commandData);

        CommandSerializers.serialize(msg, commandData);

        out.writeShort(NabuCommand.MAGIC);
        out.writeByte(msg.getType().getCode());
        out.writeInt(commandData.readableBytes());
        out.writeBytes(commandData);
    }
}
