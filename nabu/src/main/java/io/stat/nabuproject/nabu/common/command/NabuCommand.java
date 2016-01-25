package io.stat.nabuproject.nabu.common.command;

import io.netty.buffer.ByteBuf;
import io.stat.nabuproject.nabu.common.response.FailResponse;
import io.stat.nabuproject.nabu.common.response.OKResponse;
import io.stat.nabuproject.nabu.common.response.QueuedResponse;
import io.stat.nabuproject.nabu.common.response.RetryResponse;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * A basic Nabu command.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode
public abstract class NabuCommand {
    public static final short MAGIC = 0x4E43;
    public static final String MAGIC_HEX_STRING = "0x" + Integer.toHexString(MAGIC);

    private final @Getter Type type;
    private final @Getter long sequence;

    protected NabuCommand(Type type, long sequence) {
        this.type = type;
        this.sequence = sequence;
    }

    public final void encodeHeader(ByteBuf out) {
        out.writeShort(MAGIC);
        out.writeByte(getType().getCode());
        out.writeLong(getSequence());
    }

    public final OKResponse okResponse(String id) {
        return new OKResponse(getSequence(), id);
    }

    public final RetryResponse retryResponse(String id) {
        return new RetryResponse(getSequence(), id);
    }

    public final QueuedResponse queuedResponse(String id) {
        return new QueuedResponse(getSequence(), id);
    }

    public final FailResponse failResponse(String id, String reason) {
        return new FailResponse(getSequence(), id, reason);
    }

    /**
     * Represents a Type code for a NabuCommand
     */
    public enum Type {
        IDENTIFY(5),
        INDEX(10),
        UPDATE(20);

        final @Getter byte code;

        Type(int code) {
            this.code = ((byte) code);
        }

        private static final Map<Byte, Type> _responseLUT;

        static {
            Map<Byte, Type> lutBuilder = new HashMap<>();
            EnumSet.allOf(Type.class).forEach(r -> lutBuilder.put(r.getCode(), r));
            _responseLUT = Collections.unmodifiableMap(lutBuilder);
        }

        public static Type ofCode(byte value) throws IllegalArgumentException {
            Type ret = _responseLUT.get(value);

            if(ret == null) {
                throw new IllegalArgumentException("Unknown Nabu command type code " + value);
            }

            return ret;
        }
    }
}
