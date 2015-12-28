package io.stat.nabuproject.common;

import lombok.Getter;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public enum NabuResponse {
    OK(0),
    QUEUED(1),
    RETRY(2),
    FAIL(3);

    @Getter private final byte code;

    public static final short MAGIC = 0x4E52;
    public static final String MAGIC_HEX_STRING = "0x"+Integer.toHexString(MAGIC);
    private static final Map<Byte, NabuResponse> _responseLUT;

    static {
        Map<Byte, NabuResponse> lutBuilder = new HashMap<>();
        EnumSet.allOf(NabuResponse.class).forEach(r -> lutBuilder.put(r.getCode(), r));
        _responseLUT = Collections.unmodifiableMap(lutBuilder);
    }

    NabuResponse(int code) {
        this.code = ((byte)code);
    }

    @Override
    public String toString() {
        String label = super.toString();
        return "NabuResponse<" + getCode() + " " + label + ">";
    }

    public static NabuResponse ofCode(byte value) throws IllegalArgumentException {
        NabuResponse ret = _responseLUT.get(value);

        if(ret == null) {
            throw new IllegalArgumentException("Unknown NabuResponse code " + value);
        }

        return ret;
    }
}
