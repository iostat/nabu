package io.stat.nabuproject.core.enkiprotocol;

import lombok.EqualsAndHashCode;

/**
 * A command that tells a Nabu to stop consuming a Kafka partition.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode(callSuper = true)
public class EnkiUnassign extends EnkiAssign {
    public EnkiPacketType getType() { return EnkiPacketType.UNASSIGN; }
    EnkiUnassign(long sequenceNumber, String index, int partition) { super(sequenceNumber, index, partition); }
}
