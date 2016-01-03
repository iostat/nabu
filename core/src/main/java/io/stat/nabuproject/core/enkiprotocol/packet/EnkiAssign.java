package io.stat.nabuproject.core.enkiprotocol.packet;

import com.google.common.base.MoreObjects;
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

    @Override
    public String toString() {
        return MoreObjects
                .toStringHelper(this)
                .add("sequence", sequenceNumber)
                .add("topic", indexName)
                .add("partition", partitionNumber)
                .toString();
    }
}
