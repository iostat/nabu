package io.stat.nabuproject.core.enkiprotocol.packet;

import com.google.common.base.MoreObjects;

/**
 * The inverse of {@link EnkiAck}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiNak extends EnkiPacket {
    public EnkiNak(long sequenceNumber) {
        super(sequenceNumber);
    }

    @Override
    public Type getType() {
        return Type.NAK;
    }

    @Override
    public String toString() {
        return MoreObjects
                .toStringHelper(this)
                .add("sequence", sequenceNumber)
                .toString();
    }
}
