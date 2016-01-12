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
    private final @Getter String indexName;
    private final @Getter String topicName;
    private @Getter @Setter int shards = -1;
    private @Getter @Setter int partitions = -1;

    @Override
    public String toString() {
        return String.format("ESKSP<es:%s[%s], kp:%s[%s]>", getIndexName(), getShards(), getTopicName(), getPartitions());
    }
}
