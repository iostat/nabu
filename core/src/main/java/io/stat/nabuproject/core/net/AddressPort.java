package io.stat.nabuproject.core.net;

import com.google.common.base.MoreObjects;
import io.stat.nabuproject.core.util.Tuple;

import java.io.Serializable;
import java.util.Objects;

/**
 * A simple immutable {@link Tuple} which holds an address
 * and a port.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class AddressPort extends Tuple<String, Integer> implements Serializable {
    private static final long serialVersionUID = -1566911022466119392L;

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

    @Override
    public int hashCode() {
        return Objects.hash(first(), second());
    }
}
