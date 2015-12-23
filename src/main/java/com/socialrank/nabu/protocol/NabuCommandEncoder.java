package com.socialrank.nabu.protocol;

import com.socialrank.nabu.common.NabuCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class NabuCommandEncoder extends MessageToByteEncoder<NabuCommand> {
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
