package io.stat.nabuproject.enki.integration;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * ElasticSearchKafkaShardPartition.
 * No, I couldn't think of a better name.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor
@EqualsAndHashCode
final class ESKSP {
    /**
     * The name of the ES index.
     */
    private final @Getter String indexName;

    /**
     * The name of the Kafka topic
     */
    private final @Getter String topicName;

    /**
     * The number of primary shards in the ES index
     */
    private @Getter @Setter int shards = -1;

    /**
     * The number of partitions in the Kafka topic
     */
    private @Getter @Setter int partitions = -1;

    @Override
    public String toString() {
        return String.format("ESKSP<es:%s[%s], kp:%s[%s]>", getIndexName(), getShards(), getTopicName(), getPartitions());
    }
}
