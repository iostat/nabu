package io.stat.nabuproject.core.enkiprotocol;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * A command that tells a Nabu instance to start consuming a Kafka topic + partition.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
@EqualsAndHashCode(callSuper = false)
public class EnkiAssign extends EnkiPacket {
    public EnkiPacketType getType() { return EnkiPacketType.ASSIGN; }
    private final @Getter String indexName;
    private final @Getter int partitionNumber;

}
