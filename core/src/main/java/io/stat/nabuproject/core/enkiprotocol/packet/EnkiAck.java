package io.stat.nabuproject.core.enkiprotocol.packet;

/**
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiAck extends EnkiPacket {
    public EnkiAck(long sequenceNumber) {
        super(sequenceNumber);
    }

    @Override
    public EnkiPacketType getType() {
        return EnkiPacketType.ACK;
    }
}
