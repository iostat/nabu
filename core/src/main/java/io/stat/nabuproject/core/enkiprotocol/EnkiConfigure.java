package io.stat.nabuproject.core.enkiprotocol;

import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;

/**
 * A command that sets a worker's runtime configuration options.
 */
@EqualsAndHashCode(callSuper = false)

public class EnkiConfigure extends EnkiPacket {
    EnkiConfigure(long sequenceNumber, Map<String, Serializable> src){
        super(sequenceNumber);
        this.options = ImmutableMap.copyOf(src);
    }

    @Override
    public EnkiPacketType getType() {
        return EnkiPacketType.CONFIGURE;
    }

    private final @Getter ImmutableMap<String, Serializable> options;
}