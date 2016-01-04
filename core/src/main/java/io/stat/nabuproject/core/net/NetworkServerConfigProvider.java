package io.stat.nabuproject.core.net;

/**
 * Something which can provide configuration for a network server.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface NetworkServerConfigProvider {
    /**
     * @return how big the thread pool for accepting and initializing connections should be
     */
    int getAcceptorThreads();

    /**
     * @return how many worker threads should exist to service existing connections
     */
    int getWorkerThreads();

    /**
     * @return the address and point on which to listen to connections for
     */
    AddressPort getListenBinding();
}
