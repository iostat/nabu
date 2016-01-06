package io.stat.nabuproject.core.enkiprotocol.packet;

import com.google.common.base.MoreObjects;
import io.stat.nabuproject.core.net.AddressPort;
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
     * Where the redirect points to
     */
    private @Getter AddressPort destination;


    /**
     * Create a new EnkiRedirect
     * @param sequence the sequence number
     * @param address the address
     * @param port the port
     */
    public EnkiRedirect(long sequence, String address, int port) {
        this(sequence, new AddressPort(address, port));
    }

    /**
     * Create a new EnkiRedirect from an {@link AddressPort}
     * @param sequence the sequence number
     * @param ap the AddressPort
     */
    public EnkiRedirect(long sequence, AddressPort ap) {
        super(sequence);
        this.destination = ap;
    }

    /**
     * @see AddressPort#getAddress()
     */
    public String getAddress() {
        return destination.getAddress();
    }

    /**
     * @see AddressPort#getPort()
     */
    public int getPort() {
        return destination.getPort();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("sequence", sequenceNumber)
                .add("destination", destination)
                .toString();
    }
}
