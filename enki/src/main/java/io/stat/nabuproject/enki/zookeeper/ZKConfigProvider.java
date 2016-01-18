package io.stat.nabuproject.enki.zookeeper;

import java.util.List;

/**
 * Something which provides configuration for the ZooKeeper-based
 * leader election system.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ZKConfigProvider {
    /**
     * The ZooKeeper servers on which to perform leader election. (in the form of host:port)
     * @return the address of the ZK server to perform leader election in
     */
    List<String> getZookeepers();

    /**
     * A directory which will be the base of the ZK election nodes.
     * That is, the final path where ZNodes will be creates is LEZKServer:LEZKPort/LEZKChroot/enki_le/***
     * @return the ZKChroot
     */
    String getZKChroot();

    /**
     * How long to wait before the connection times out.
     * @return the zookeeper connection timeout.
     */
    int getZKConnectionTimeout();
}
