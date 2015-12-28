package io.stat.nabuproject.core.enkiprotocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToByteEncoder;
import io.stat.nabuproject.core.util.ProtocolHelper;

import java.util.List;

/**
 * An abstract Enki packet. Really just holds the MAGIC.
 * And the encoder. And the decoder.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class EnkiPacket {
    public static final int MAGIC = 0x454E4B49;
    public static final String MAGIC_HEX_STRING = "0x" + Integer.toHexString(MAGIC);

    public abstract EnkiPacketType getType();

    public static final class Encoder extends MessageToByteEncoder<EnkiPacket> {
        @Override
        protected void encode(ChannelHandlerContext ctx, EnkiPacket msg, ByteBuf out) throws Exception {
            out.writeInt(MAGIC);

            EnkiPacketType packetType = msg.getType();
            out.writeInt(packetType.getCode());

            if(packetType == EnkiPacketType.HEARTBEAT) {
                out.writeLong(((EnkiHeartbeat)msg).getTimestamp());
            } else {
                ByteBuf restOfPacket = Unpooled.buffer();

                EnkiAssign casted = ((EnkiAssign)msg);
                restOfPacket.writeInt(casted.getPartitionNumber());
                ProtocolHelper.writeStringToByteBuf(casted.getIndexName(), restOfPacket);

                out.writeInt(restOfPacket.readableBytes());
                out.writeBytes(restOfPacket);
            }
        }
    }

    public static final class Decoder extends ByteToMessageDecoder {

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            // needs at least 16 bytes (2 ints for MAGIC, then CODE
            // followed by at least a long for timestamp if its a HEARTBEAT
            // or REST_OF_PACKET_SIZE and PARTITION_NUMBER (2x int)
            if(in.readableBytes() < 16) {
                return;
            }

            in.markReaderIndex();

            int magic = in.readInt();
            if(magic != MAGIC) {
                in.resetReaderIndex();
                throw new CorruptedFrameException("Invalid command magic. " +
                        "Received 0x" + Integer.toHexString(magic)
                        + " but expected" + MAGIC_HEX_STRING);
            }

            int packetTypeCode = in.readInt();
            EnkiPacketType packetType;
            try {
                packetType = EnkiPacketType.ofCode(packetTypeCode);
            } catch(IllegalArgumentException e) {
                in.resetReaderIndex();
                throw new CorruptedFrameException(e);
            }

            if(packetType == EnkiPacketType.HEARTBEAT) {
                if(in.readableBytes() < 8) {
                    in.resetReaderIndex();
                    return;
                }

                out.add(new EnkiHeartbeat(in.readLong()));
            } else {
                int restOfPacketSize = in.readInt();
                if(in.readableBytes() < restOfPacketSize) {
                    in.resetReaderIndex();
                    return;
                }

                int partitionNumber = in.readInt();
                String indexName = ProtocolHelper.readStringFromByteBuf(in);

                out.add(
                        (packetType == EnkiPacketType.ASSIGN)        ?
                        new EnkiAssign(indexName, partitionNumber)   :
                        new EnkiUnassign(indexName, partitionNumber)
                );
            }
        }
    }
}
