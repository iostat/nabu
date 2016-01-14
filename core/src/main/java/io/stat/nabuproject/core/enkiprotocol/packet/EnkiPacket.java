package io.stat.nabuproject.core.enkiprotocol.packet;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToByteEncoder;
import io.stat.nabuproject.core.net.ObjectDecoderExposer;
import io.stat.nabuproject.core.net.ObjectEncoderExposer;
import io.stat.nabuproject.core.net.ProtocolHelper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * An abstract Enki packet. Really just holds the MAGIC.
 * And the encoder. And the decoder.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode
public abstract class EnkiPacket {
    public static final int MAGIC = 0x454E4B49;
    public static final String MAGIC_HEX_STRING = "0x" + Integer.toHexString(MAGIC);

    public abstract Type getType();
    public final @Getter long sequenceNumber;

    protected EnkiPacket(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String toString() {
        return MoreObjects
                .toStringHelper(this)
                .add("sequence", sequenceNumber)
                .toString();
    }

    @Slf4j
    public static final class Encoder extends MessageToByteEncoder<EnkiPacket> {

        private final ObjectEncoderExposer exposer;

        public Encoder() {
            this.exposer = new ObjectEncoderExposer();
        }

        @Override
        protected void encode(ChannelHandlerContext ctx, EnkiPacket msg, ByteBuf out) throws Exception {
            Type packetType = msg.getType();

            // ACK NAK and LEAVE have no special data attached to them.
            if(packetType == Type.ACK || packetType == Type.NAK || packetType == Type.LEAVE) {
                out.writeInt(MAGIC);
                out.writeInt(packetType.getCode());
                out.writeLong(msg.getSequenceNumber());
                return;
            }

            // Heartbeats are very simple to serialize.
            if(packetType == Type.HEARTBEAT) {
                out.writeInt(MAGIC);
                out.writeInt(packetType.getCode());
                out.writeLong(msg.getSequenceNumber());
                out.writeLong(((EnkiHeartbeat)msg).getTimestamp());
                return;
            }

            // Everything else brings us pain and suffering.
            ByteBuf restOfPacket = Unpooled.buffer();
            switch(packetType) {
                case ASSIGN:
                case UNASSIGN:
                    EnkiAssign assign = ((EnkiAssign) msg);

                    restOfPacket.writeInt(assign.getPartitionNumber());
                    ProtocolHelper.writeStringToByteBuf(assign.getIndexName(), restOfPacket);
                    break;
                case CONFIGURE:
                    ImmutableMap<String, Serializable> configure = ((EnkiConfigure) msg).getOptions();
                    ProtocolHelper.writeSerializableToByteBuf(configure, restOfPacket);
                    break;
                case REDIRECT:
                    EnkiRedirect redir = ((EnkiRedirect) msg);
                    ProtocolHelper.writeStringToByteBuf(redir.getAddress(), restOfPacket);
                    restOfPacket.writeInt(redir.getPort());
                    break;
                default:
                    throw new RuntimeException("Don't know how to handle EnkiPacket of type " + packetType);
            }

            out.writeInt(MAGIC);
            out.writeInt(packetType.getCode());
            out.writeLong(msg.getSequenceNumber());
            out.writeInt(restOfPacket.readableBytes());
            out.writeBytes(restOfPacket);

            restOfPacket.release();
        }
    }

    @Slf4j
    public static final class Decoder extends ByteToMessageDecoder {

        private final ObjectDecoderExposer exposer;

        public Decoder() {
            this.exposer = new ObjectDecoderExposer();
        }


        // for EnkiConfigures it has to decode what should be a Map<String, Serializable>
        // but alas ObjectEncoder isn't exactly generic.
        @SuppressWarnings("unchecked")
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            // needs at least 16 bytes (2 ints for MAGIC, then CODE
            // then a long sequenceNumber
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
            Type packetType;
            try {
                packetType = Type.ofCode(packetTypeCode);
            } catch(IllegalArgumentException e) {
                in.resetReaderIndex();
                throw new CorruptedFrameException(e);
            }
            long sequenceNumber = in.readLong();

            // if the type is an ACK, NAK, or LEAVE it has no other data attached to it.
            // otherwise, it's followed by at least a long for timestamp if its a HEARTBEAT
            // or REST_OF_PACKET_SIZE and PARTITION_NUMBER (2x int)
            if(packetType == Type.ACK) {
                out.add(new EnkiAck(sequenceNumber));
                return;
            } else if (packetType == Type.NAK) {
                out.add(new EnkiNak(sequenceNumber));
                return;
            } else if (packetType == Type.LEAVE) {
                out.add(new EnkiLeave(sequenceNumber));
                return;
            } else if(packetType == Type.HEARTBEAT) {
                if(in.readableBytes() < 8) {
                    in.resetReaderIndex();
                    return;
                }

                out.add(new EnkiHeartbeat(sequenceNumber, in.readLong()));
                return;
            } else {
                if(in.readableBytes() < 4) { // need int for LENGTH
                    in.resetReaderIndex();
                    return;
                }
                int restOfPacketSize = in.readInt();
                if(in.readableBytes() < restOfPacketSize) {
                    in.resetReaderIndex();
                    return;
                }
                switch(packetType) {
                    case ASSIGN:
                    case UNASSIGN:
                        int partitionNumber = in.readInt();
                        String indexName = ProtocolHelper.readStringFromByteBuf(in);

                        out.add(
                                (packetType == Type.ASSIGN)                                ?
                                        new EnkiAssign(sequenceNumber, indexName, partitionNumber)   :
                                        new EnkiUnassign(sequenceNumber, indexName, partitionNumber)
                        );
                        return;
                    case CONFIGURE:
                        ImmutableMap<String, Serializable> decoded = ProtocolHelper.readSerializableFromByteBuf(in);
                        out.add(new EnkiConfigure(sequenceNumber, decoded));

                        return;
                    case REDIRECT:
                        String address = ProtocolHelper.readStringFromByteBuf(in);
                        int port = in.readInt();

                        out.add(new EnkiRedirect(sequenceNumber, address, port));
                        return;
                    default:
                        throw new CorruptedFrameException("Don't know how to handle EnkiPacket of type " + packetType);
                }
            }
        }
    }

    /**
     * An enum of all possible {@link EnkiPacket} types. Used
     * in conjunction with {@link EnkiPacket#getType()}
     *
     * @author Ilya Ostrovskiy (https://github.com/iostat/)
     */
    public enum Type {
        /**
         * @see EnkiHeartbeat
         */
        HEARTBEAT(0),
        /**
         * @see EnkiAssign
         */
        ASSIGN(1),
        /**
         * @see EnkiUnassign
         */
        UNASSIGN(2),
        /**
         * @see EnkiConfigure
         */
        CONFIGURE(3),
        /**
         * @see EnkiLeave
         */
        LEAVE(5),
        /**
         * @see EnkiAck
         */
        ACK(100),

        /**
         * An awesome pun on so many levels
         * @see EnkiRedirect
         */
        REDIRECT(303),

        /**
         * @see EnkiNak
         */
        NAK(999);

        private final @Getter int code;

        private static final Map<Integer, Type> _LUT;

        Type(int code) {
            this.code = code;
        }

        static {
            ImmutableMap.Builder<Integer, Type> lutBuilder = ImmutableMap.builder();
            EnumSet.allOf(Type.class).forEach(type -> lutBuilder.put(type.getCode(), type));
            _LUT = lutBuilder.build();
        }

        public static Type ofCode(int value) throws IllegalArgumentException {
            Type ret = _LUT.get(value);

            if(ret == null) {
                throw new IllegalArgumentException("Unknown EnkiPacketType code " + value);
            }

            return ret;
        }
    }

}
