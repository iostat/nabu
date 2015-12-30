package io.stat.nabuproject.core.enkiprotocol;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A command that tells a Nabu instance to start consuming a Kafka topic + partition.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode(callSuper = true)
public class EnkiAssign extends EnkiPacket {
    public EnkiPacketType getType() { return EnkiPacketType.ASSIGN; }
    private final @Getter String indexName;
    private final @Getter int partitionNumber;

    EnkiAssign(long sequenceNumber, String indexName, int partitionNumber) {
        super(sequenceNumber);
        this.indexName = indexName;
        this.partitionNumber = partitionNumber;
    }
}
