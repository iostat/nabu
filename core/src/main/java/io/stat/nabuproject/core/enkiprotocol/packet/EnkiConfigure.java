package io.stat.nabuproject.core.enkiprotocol.packet;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
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
    public EnkiConfigure(long sequenceNumber, Map<String, Serializable> src){
        super(sequenceNumber);
        this.options = ImmutableMap.copyOf(src);
    }

    @Override
    public Type getType() {
        return Type.CONFIGURE;
    }

    private final @Getter ImmutableMap<String, Serializable> options;

    @Override
    public String toString() {
        return MoreObjects
                .toStringHelper(this)
                .add("sequence", sequenceNumber)
                .add("config", "{" + Joiner.on(',').withKeyValueSeparator("=").join(options) + "}")
                .toString();
    }
}
