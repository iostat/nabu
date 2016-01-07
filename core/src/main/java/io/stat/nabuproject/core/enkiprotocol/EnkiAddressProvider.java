package io.stat.nabuproject.core.enkiprotocol;

import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.util.Tuple;

import java.util.List;

/**
 * Provides the IP address and port of the master Enki node.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface EnkiAddressProvider {
    /**
     * Whether or not there is an Enki advertised in the cluster
     * that we can connect to.
     * @return whether or not this provider knows of an Enki.
     */
    default boolean isEnkiDiscovered() {
        return false;
    }

    /**
     * Gets a list of all Enkis that this provider is aware of.
     * The first element of the tuple is the Enki's ES node name, and
     * the second element is its AddressPort
     * @return a list of all Enkis that this provider is aware of.
     */
    default List<Tuple<String, AddressPort>> getDiscoveredEnkis() { return null; }
}
