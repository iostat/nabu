package io.stat.nabuproject.core.kafka;

import java.util.List;

/**
 * Something that can provide configuration to aspects of the Kafka subsystem
 * which interact directly with the ZooKeeper that backs Kafka.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface KafkaZkConfigProvider {
    /**
     * A list of <tt>address:port</tt> Strings containing the location of ZooKeeper
     * instances
     * @return a list of zookeeper server <tt>address:port</tt> Strings
     */
    List<String> getKafkaZookeepers();

    /**
     * If Kafka is not operating in the root of the ZooKeeper tree,
     * the subdirectory it is chroot'ed to, without the trailing slash
     * @return the chroot, if any, or blank.
     */
    String getKafkaZkChroot();

    /**
     * The timeout for attempting to connect to ZooKeeper, in seconds
     * @return how long to wait in seconds before a connection attempt times out.
     */
    int getKafkaZkConnTimeout();
}
