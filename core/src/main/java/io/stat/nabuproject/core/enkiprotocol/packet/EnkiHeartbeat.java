package io.stat.nabuproject.core.enkiprotocol.packet;

import com.google.common.base.MoreObjects;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A heartbeat packet. These are sent fairly frequently
 * by the server to the client to ensure that it is still alive.
 *
 * It is responded to by an ACK with the same sequence number.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode(callSuper = true)
public final class EnkiHeartbeat extends EnkiPacket {
    public Type getType() { return Type.HEARTBEAT; }
    private final @Getter long timestamp;

    public EnkiHeartbeat(long sequenceNumber) {
        this(sequenceNumber, System.currentTimeMillis());
    }

    EnkiHeartbeat(long sequenceNumber, long timestamp) {
        super(sequenceNumber);
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return MoreObjects
                .toStringHelper(this)
                .add("sequence", sequenceNumber)
                .add("timestamp", timestamp)
                .toString();
    }
}
