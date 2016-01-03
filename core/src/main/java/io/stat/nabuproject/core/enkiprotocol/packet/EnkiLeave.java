package io.stat.nabuproject.core.enkiprotocol.packet;

import com.google.common.base.MoreObjects;

/**
 * If this packet is sent BY Enki TO Nabu, it means that
 * the node should stop all throttled writes and shut down.
 *
 * If sent BY Nabu TO Enki, it means that the Nabu is shutting down,
 * and has already stopped all throttled writing.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiLeave extends EnkiPacket {
    public EnkiLeave(long sequenceNumber) {
        super(sequenceNumber);
    }

    @Override
    public Type getType() {
        return Type.LEAVE;
    }

    @Override
    public String toString() {
        return MoreObjects
                .toStringHelper(this)
                .add("sequence", sequenceNumber)
                .toString();
    }
}

