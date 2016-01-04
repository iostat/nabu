package io.stat.nabuproject.core.net;

import com.google.common.base.MoreObjects;
import io.stat.nabuproject.core.util.Tuple;
import lombok.EqualsAndHashCode;

/**
 * A simple immutable {@link Tuple} which holds an address
 * and a port.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode(callSuper=false)
public class AddressPort extends Tuple<String, Integer> {
    public AddressPort(String address, int port) {
        super(address, port);
    }

    public String getAddress() {
        return first();
    }

    public int getPort() {
        return second();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(AddressPort.class)
                .add("address", getAddress())
                .add("port", getPort())
                .toString();
    }
}
