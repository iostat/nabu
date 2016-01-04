package io.stat.nabuproject.core.enkiprotocol.packet;

import com.google.common.base.MoreObjects;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Sent by the server to tell the client that they are not the current
 * master, and they should use the location specified to contact the current master.
 *
 * If this packet is sent after some ASSIGNs have been made, the client should act as
 * though all those tasks were UNASSIGN'd prior to this packet.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode(callSuper=true)
public class EnkiRedirect extends EnkiPacket {
    @Override
    public Type getType() {
        return Type.REDIRECT;
    }

    /**
     * The address that the client should connect to.
     */
    private final @Getter String address;

    /**
     * The port that the client should connect to.
     */
    private final @Getter int port;

    public EnkiRedirect(long sequence, String address, int port) {
        super(sequence);
        this.address = address;
        this.port = port;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("sequence", sequenceNumber)
                .add("address", address)
                .add("port", port)
                .toString();
    }
}
