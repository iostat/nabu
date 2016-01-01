package io.stat.nabuproject.core.enkiprotocol;

/**
 * Provides the IP address and port of the master Enki node.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface EnkiAddressProvider {
    /**
     * Whether or not there is an Enki advertised in the cluster
     * @return whether or not this provider knows of an Enki.
     */
    default boolean isEnkiDiscovered() {
        return false;
    }

    /**
     * The hostname or IP address of the Enki master instance.
     * @return the hostname or IP address of the Enki master
     *         or null if {@link EnkiAddressProvider#isEnkiDiscovered()} is false.
     */
    default String getEnkiHost() {
        return null;
    }

    /**
     * The port of the Enki master instance that serves the Enki protocol.
     * @return the port an {@link EnkiClient} should connect to reach the Enki master
     *         or -1 if {@link EnkiAddressProvider#isEnkiDiscovered()} is false.
     */
    default int getEnkiPort() {
        return -1;
    }
}
