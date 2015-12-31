package io.stat.nabuproject.core.enkiprotocol.packet;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A heartbeat request OR response.
 * Just needs to be sent back within a reasonable time.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode(callSuper = true)
public final class EnkiHeartbeat extends EnkiPacket {
    public EnkiPacketType getType() { return EnkiPacketType.HEARTBEAT; }
    private final @Getter long timestamp;

    public EnkiHeartbeat(long sequenceNumber) {
        this(sequenceNumber, System.currentTimeMillis());
    }

    EnkiHeartbeat(long sequenceNumber, long timestamp) {
        super(sequenceNumber);
        this.timestamp = timestamp;
    }
}
