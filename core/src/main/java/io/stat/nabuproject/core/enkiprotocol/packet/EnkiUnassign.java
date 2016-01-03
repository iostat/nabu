package io.stat.nabuproject.core.enkiprotocol.packet;

import com.google.common.base.MoreObjects;
import lombok.EqualsAndHashCode;

/**
 * A command sent by Enki that tells a Nabu to stop consuming a Kafka partition.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode(callSuper = true)
public class EnkiUnassign extends EnkiAssign {
    public Type getType() { return Type.UNASSIGN; }
    public EnkiUnassign(long sequenceNumber, String index, int partition) { super(sequenceNumber, index, partition); }

    @Override
    public String toString() {
        return MoreObjects
                .toStringHelper(this)
                .add("sequence", sequenceNumber)
                .toString();
    }
}
