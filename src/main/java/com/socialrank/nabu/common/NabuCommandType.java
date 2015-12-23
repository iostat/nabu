package com.socialrank.nabu.common;

import lombok.Getter;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public enum NabuCommandType {
    INDEX(0),
    UPDATE(1);

    @Getter byte code;

    NabuCommandType(int code) {
        this.code = ((byte) code);
    }

    private static final Map<Byte, NabuCommandType> _responseLUT;

    static {
        Map<Byte, NabuCommandType> lutBuilder = new HashMap<>();
        EnumSet.allOf(NabuCommandType.class).forEach(r -> lutBuilder.put(r.getCode(), r));
        _responseLUT = Collections.unmodifiableMap(lutBuilder);
    }

    public static NabuCommandType ofCode(byte value) throws IllegalArgumentException {
        NabuCommandType ret = _responseLUT.get(value);

        if(ret == null) {
            throw new IllegalArgumentException("Unknown NabuCommandType code " + value);
        }

        return ret;
    }
}
