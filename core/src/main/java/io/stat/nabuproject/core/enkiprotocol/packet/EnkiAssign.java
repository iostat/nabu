package io.stat.nabuproject.core.enkiprotocol.packet;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A command sent by Enki to Nabu that tells the instance to
 * start consuming a Kafka topic + partition.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode(callSuper = true)
public class EnkiAssign extends EnkiPacket {
    public Type getType() { return Type.ASSIGN; }
    private final @Getter String indexName;
    private final @Getter int partitionNumber;

    EnkiAssign(long sequenceNumber, String indexName, int partitionNumber) {
        super(sequenceNumber);
        this.indexName = indexName;
        this.partitionNumber = partitionNumber;
    }
}
