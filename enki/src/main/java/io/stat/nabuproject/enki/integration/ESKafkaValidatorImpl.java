package io.stat.nabuproject.enki.integration;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.elasticsearch.ESException;
import io.stat.nabuproject.core.kafka.KafkaMetadataClient;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Map;
import java.util.stream.Collectors;

import static io.stat.nabuproject.core.util.functional.FluentCompositions.$;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.bridge;
import static io.stat.nabuproject.core.util.functional.FluentCompositions.fmap;

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
class ESKafkaValidatorImpl extends ESKafkaValidator {
    private final ESClient esClient;
    private final ThrottlePolicyProvider config;
    private final KafkaMetadataClient kafkaClient;
    private @Getter boolean isSane = false;
    private Map<String, ESKSP> shardCountCache = null;

    @Override
    public void start() throws ComponentException {
        Map<String, ESKSP> esksps = getShardCountCache();

        GetIndexResponse indexInfo;
        try {
            indexInfo = esClient.getIndexMetadata(fmap(ESKSP::getIndexName, esksps.values())
                                                      .toArray(String[]::new));
        } catch(ESException e) {
            Throwable cause = e.getCause();
            if(cause instanceof IndexNotFoundException) {
                IndexNotFoundException realCause = ((IndexNotFoundException)cause);
                String message = String.format("Index configured for throttling %s does not exist in ElasticSearch", realCause.getIndex());
                throw new ComponentException(true, message, e);
            } else {
                throw new ComponentException(true, "Received an unexpected exception when querying index shard counts", e);
            }
        }

        for(ObjectObjectCursor<String, Settings> cursor : indexInfo.settings()) {
            String indexName = cursor.key;
            String s_nos = cursor.value.get(ESClient.INDEX_NUMBER_OF_PRIMARY_SHARDS_SETTING);

            int shardCount;
            try {
                shardCount = Integer.parseInt(s_nos);
            } catch(NullPointerException | NumberFormatException n) {
                if(s_nos == null) s_nos = "<null>";
                throw new ComponentException(true, String.format("Received an invalid shard count %s for index %s from ES", indexName, s_nos), n);
            }

            logger.info("Index {} in ElasticSearch has {} shard(s)", indexName, shardCount);

            esksps.get(indexName).setShards(shardCount);
        }

        for(ESKSP esksp : esksps.values()) {
            String topicName = esksp.getTopicName();
            String indexName = esksp.getIndexName();
            int expectedPartitions = esksp.getShards();
            int actualPartitions;
            boolean topicExists;

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
                esksp.setPartitions(actualPartitions);
            } catch(Exception e) {
                throw new ComponentException(true,
                        String.format("Failed to look up partition counts for Kafka topic %s (for index %s)", topicName, indexName),
                        e);
            }

            if(actualPartitions != expectedPartitions) {
                throw new ComponentException(true,
                        String.format("Mismatch between shard count for %s",
                                esksp));
            }

            logger.info("Integration check passed: {}", esksp);
        }

        this.isSane = true;
        logger.info("Integration Sanity Check passed successfully!");
    }

    @Override
    Map<String, ESKSP> getShardCountCache() {
        if(this.shardCountCache == null) {
            this.shardCountCache = config.getThrottlePolicies()
                                         .stream()
                                         .map(bridge(ThrottlePolicy::getIndexName,
                                                     ThrottlePolicy::getTopicName,
                                                     ESKSP::new))
                                         .collect(Collectors.toMap(
                                                    ESKSP::getIndexName,
                                                    $()));
        }

        return this.shardCountCache;
    }
}
