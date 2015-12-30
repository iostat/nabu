package io.stat.nabuproject.enki.state;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.config.ThrottlePolicy;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.enki.EnkiConfig;
import io.stat.nabuproject.enki.kafka.KafkaMetadataClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

/**
 * A component which will query ElasticSearch and Kafka and
 * ensure that topics exist for indices that should be throttled
 * and that the count of partitions and shards match up.
 *
 * If there's a failure, it throws a fatal ComponentException
 * which will cause Enki to shut down.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor(onConstructor=@__(@Inject))
@Slf4j
public class IntegrationSanityChecker extends Component {
    private final ESClient esClient;
    private final EnkiConfig config;
    private final KafkaMetadataClient kafkaClient;

    @Override
    public void start() throws ComponentException {
        String[] targetedIndicies = config.getThrottlePolicies()
                                          .stream()
                                          .map(ThrottlePolicy::getIndexName)
                                          .toArray(String[]::new);

        ImmutableMap.Builder<String, Integer> topicPartitionCountBuilder = ImmutableMap.builder();
        GetIndexResponse indexInfo;
        try {
            indexInfo = esClient.getESClient()
                    .admin()
                    .indices()
                    .prepareGetIndex().setIndices(targetedIndicies)
                    .get();
        } catch (IndexNotFoundException e) {
            String message = String.format("Index configured for throttling %s does not exist in ElasticSearch", e.getIndex());
            throw new ComponentException(true, message, e);
        } catch (Exception e) {
            throw new ComponentException(true, "Received an unexpected exception when querying index shard counts", e);
        }

        for(ObjectObjectCursor<String, Settings> cursor : indexInfo.settings()) {
            String indexName = cursor.key;
            String s_nos = cursor.value.get("index.number_of_shards");

            int shardCount;
            try {
                shardCount = Integer.parseInt(s_nos);
            } catch(NullPointerException | NumberFormatException n) {
                if(s_nos == null) s_nos = "<null>";
                throw new ComponentException(true, String.format("Received an invalid shard count %s for index %s from ES", indexName, s_nos), n);
            }

            logger.info("Index {} in ElasticSearch has {} shard(s)", indexName, shardCount);

            topicPartitionCountBuilder.put(indexName, shardCount);
        }

        ImmutableMap<String, Integer> topicPartitionCounts = topicPartitionCountBuilder.build();
        for(String indexName : topicPartitionCounts.keySet()) {
            String topicName = "nabu-"+indexName;
            int expectedPartitions = topicPartitionCounts.get(indexName);
            int actualPartitions = -1;
            boolean topicExists = false;

            try {
                topicExists = kafkaClient.topicExists(topicName);
            } catch(Exception e) {
                throw new ComponentException(true,
                        String.format("Failed to look up if Kafka topic %s exists for index %s", topicName, indexName),
                        e);
            }

            if(!topicExists) {
                throw new ComponentException(true,
                        String.format("Kafka topic %s for index %s does not exist!", topicName, indexName));
            }

            try {
                actualPartitions = kafkaClient.topicPartitionsCount(topicName);
            } catch(Exception e) {
                throw new ComponentException(true,
                        String.format("Failed to look up partition counts for Kafka topic %s exists (for index %s)", topicName, indexName),
                        e);
            }

            if(actualPartitions != expectedPartitions) {
                throw new ComponentException(true,
                        String.format("Mismatch between shard count for index %s (%d shards) " +
                                "and partition count for topic %s (%d partitions)",
                                indexName, expectedPartitions, topicName, actualPartitions));
            }

            logger.info("Integration check passed: es:{}[{}] <-> kafka:{}[{}]", indexName, expectedPartitions, topicName, actualPartitions);
        }

        logger.info("Integration Sanity Check passed successfully!");
    }
}
