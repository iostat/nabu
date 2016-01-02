package io.stat.nabuproject.core.enkiprotocol.packet;

/**
 * A very basic packet. Sent by either Nabu or Enki to notify the other
 * that it has acknowledged a request.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiAck extends EnkiPacket {
    public EnkiAck(long sequenceNumber) {
        super(sequenceNumber);
    }

    @Override
    public Type getType() {
        return Type.ACK;
    }
}
