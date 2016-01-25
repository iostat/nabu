package io.stat.nabuproject.nabu.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.stat.nabuproject.core.net.ProtocolHelper;
import io.stat.nabuproject.nabu.common.command.NabuCommand;
import io.stat.nabuproject.nabu.common.command.NabuWriteCommand;

/**
 * Encodes {@link NabuWriteCommand}s that can be sent over the network and decoded by a
 * {@link CommandDecoder} on the other end.
 */
public class CommandEncoder extends MessageToByteEncoder<NabuCommand> {
    public byte[] performEncode(NabuCommand command) throws Exception {
        ByteBuf out = PooledByteBufAllocator.DEFAULT.buffer();
        encode(null, command, out);
        return ProtocolHelper.convertAndRelease(out);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, NabuCommand msg, ByteBuf out) throws Exception {
        if(msg instanceof NabuWriteCommand) {
            ((NabuWriteCommand)msg).encode(out);
        } else {
            // write the header and the fact that there is no more data.
            msg.encodeHeader(out);
            out.writeInt(0);
        }
    }
}
