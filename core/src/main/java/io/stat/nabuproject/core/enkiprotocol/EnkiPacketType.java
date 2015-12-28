package io.stat.nabuproject.core.enkiprotocol;

import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.EnumSet;
import java.util.Map;

/**
 * Created by io on 12/28/15. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum EnkiPacketType {
    HEARTBEAT(0),
    ASSIGN(1),
    UNASSIGN(2);

    private final @Getter int code;

    private static final Map<Integer, EnkiPacketType> _LUT;

    static {
        ImmutableMap.Builder<Integer, EnkiPacketType> lutBuilder = ImmutableMap.builder();
        EnumSet.allOf(EnkiPacketType.class).forEach(type -> lutBuilder.put(type.getCode(), type));
        _LUT = lutBuilder.build();
    }

    public static EnkiPacketType ofCode(int value) throws IllegalArgumentException {
        EnkiPacketType ret = _LUT.get(value);

        if(ret == null) {
            throw new IllegalArgumentException("Unknown EnkiPacketType code " + value);
        }

        return ret;
    }
}
