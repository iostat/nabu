package io.stat.nabuproject.core.enkiprotocol;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A heartbeat request OR response.
 * Just needs to be sent back within a reasonable time.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@EqualsAndHashCode(callSuper = false)
public final class EnkiHeartbeat extends EnkiPacket {
    public EnkiPacketType getType() { return EnkiPacketType.HEARTBEAT; }
    private final @Getter long timestamp;

    public EnkiHeartbeat() {
        this.timestamp = System.currentTimeMillis();
    }
}
