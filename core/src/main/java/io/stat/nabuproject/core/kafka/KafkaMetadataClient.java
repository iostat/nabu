package io.stat.nabuproject.core.kafka;

import io.stat.nabuproject.core.Component;

/**
 * A class which can get metadata about Kafka topics,
 * namely whether they exist and how many partitions they have.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class KafkaMetadataClient extends Component {
    /**
     * Checks whether or not the specific topic exists in Kafka
     * WITHOUT creating it if it doesn't exist.
     * @param topicName the name of the Kafka topic
     * @return whether or not it exists, without creating it
     */
    public abstract boolean topicExists(String topicName);

    /**
     * Gets the amount of partitions a topic has in Kafka,
     * WITHOUT creating it if it doesn't exist.
     * @param topicName the name of the topic
     * @return how many partitions it has in Kafka
     */
    public abstract int topicPartitionsCount(String topicName);
}
