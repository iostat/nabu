package io.stat.nabuproject.nabu.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.stat.nabuproject.core.net.ProtocolHelper;
import io.stat.nabuproject.nabu.common.response.FailResponse;
import io.stat.nabuproject.nabu.common.response.IDResponse;
import io.stat.nabuproject.nabu.common.response.NabuResponse;
import io.stat.nabuproject.nabu.common.response.OKResponse;
import io.stat.nabuproject.nabu.common.response.QueuedResponse;
import io.stat.nabuproject.nabu.common.response.RetryResponse;

/**
 * Encodes {@link NabuResponse}s into data which can be decoded by a {@link ResponseDecoder} on
 * the other side.
 */
public class ResponseEncoder extends MessageToByteEncoder<NabuResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, NabuResponse msg, ByteBuf out) throws Exception {
        out.writeShort(NabuResponse.MAGIC);
        out.writeByte(msg.getType().getCode());
        out.writeLong(msg.getSequence());

        // todo: generify and clean this garbage up
        if(msg.getType() == NabuResponse.Type.ID) {
            if(msg instanceof IDResponse) {
                ProtocolHelper.writeStringToByteBuf(((IDResponse)msg).getData(), out);
            } else {
                throw new IllegalArgumentException("Packet type is ID but packet not an instance of IDResponse!");
            }
        } else if(msg.getType() == NabuResponse.Type.FAIL) {
            if(msg instanceof FailResponse) {
                ProtocolHelper.writeStringToByteBuf(((FailResponse)msg).getId(), out);
                ProtocolHelper.writeStringToByteBuf(((FailResponse)msg).getReason(), out);
            } else {
                throw new IllegalArgumentException("Packet type is FAIL but packet not an instance of FailResponse!");
            }
        } else if(msg.getType() == NabuResponse.Type.OK) {
            if(msg instanceof OKResponse) {
                ProtocolHelper.writeStringToByteBuf(((OKResponse)msg).getId(), out);
            } else {
                throw new IllegalArgumentException("Packet type is OK but packet not an instance of OKResponse!");
            }
        } else if(msg.getType() == NabuResponse.Type.RETRY) {
            if(msg instanceof RetryResponse) {
                ProtocolHelper.writeStringToByteBuf(((RetryResponse)msg).getId(), out);
            } else {
                throw new IllegalArgumentException("Packet type is RETRY but packet not an instance of RetryResponse!");
            }
        } else if(msg.getType() == NabuResponse.Type.QUEUED) {
            if(msg instanceof QueuedResponse) {
                ProtocolHelper.writeStringToByteBuf(((QueuedResponse)msg).getId(), out);
            } else {
                throw new IllegalArgumentException("Packet type is QUEUED but packet not an instance of QueuedResponse!");
            }
        }
    }
}
