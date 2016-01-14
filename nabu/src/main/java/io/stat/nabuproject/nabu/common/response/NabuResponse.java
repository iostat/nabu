package io.stat.nabuproject.nabu.common.response;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * An abstract NabuResponse. All children except FailResponse do
 * absolutely nothing except reify an instance of this class.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor @EqualsAndHashCode
public abstract class NabuResponse {
    public static final short MAGIC = 0x4E52;
    public static final String MAGIC_HEX_STRING = "0x"+Integer.toHexString(MAGIC);

    private final @Getter Type type;
    private final @Getter long sequence;

    @Override
    public String toString() {
        return String.format("<NabuResp_%s[%d]>", getType().toString(), getSequence());
    }

    /**
     * A Enum of all possible {@link NabuResponse} types.
     * Used in conjunction with getType()
     */
    public enum Type {
        OK(0),
        QUEUED(1),
        RETRY(2),
        FAIL(3),
        ID(100);

        @Getter
        private final byte code;

        private static final Map<Byte, Type> _responseLUT;

        static {
            Map<Byte, Type> lutBuilder = new HashMap<>();
            EnumSet.allOf(Type.class).forEach(r -> lutBuilder.put(r.getCode(), r));
            _responseLUT = Collections.unmodifiableMap(lutBuilder);
        }

        Type(int code) {
            this.code = ((byte)code);
        }

        @Override
        public String toString() {
            String label = super.toString();
            return "Type<" + getCode() + " " + label + ">";
        }

        public static Type ofCode(byte value) throws IllegalArgumentException {
            Type ret = _responseLUT.get(value);

            if(ret == null) {
                throw new IllegalArgumentException("Unknown Nabu response type code " + value);
            }

            return ret;
        }
    }
}
