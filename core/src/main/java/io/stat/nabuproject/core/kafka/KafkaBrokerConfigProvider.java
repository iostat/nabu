package io.stat.nabuproject.core.kafka;

import java.util.List;

/**
 * A class that can provide the configuration of Kafka broker nodes.
 * Unlike some other ConfigProviders, this one exposes the option to set its data.
 *
 * This allows the provider to be bootstrapped with data, for instance, at runtime.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface KafkaBrokerConfigProvider {
    /**
     * @return whether or not the data this class is responsible for is data is available
     */
    boolean isKafkaBrokerConfigAvailable();

    /**
     * @return A list of addresses to contact the Kafka brokers for this Nabu cluster
     */
    List<String> getKafkaBrokers();

    /**
     * @return what group ID to use when subscribing to topics.
     */
    String getKafkaGroup();

    /**
     * Can optionally be implemented to bootstrap the data. It is expected that
     * isAvailable() will return true after this is called. The implementor can opt to not respect this call,
     * and the default implementation is a no-op.
     * @param brokers what to return for getBrokers if not already set.
     * @param group what to return for group, if not already set.
     */
    default void setKafkaBrokerConfig(List<String> brokers, String group) { }
}
