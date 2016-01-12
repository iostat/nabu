package io.stat.nabuproject.nabu.kafka;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.nabu.common.IndexCommand;
import io.stat.nabuproject.nabu.common.NabuCommand;
import io.stat.nabuproject.nabu.common.UpdateCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A managed wrapper around a KafkaConsumer that consumes only a single instance
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
final class SingleTPConsumer extends Component {
    /**
     * The ESClient which we will use to perform our write ops
     */
    private final ESClient esClient;

    /**
     * The throttle policy that specifies target write time and max batch size
     */
    private final ThrottlePolicy throttlePolicy;

    /**
     * The number of the Kafka partition to which to subscribe to
     * for the topic specificed in the throttlePolicy
     */
    private final int partitionToSubscribe;

    /**
     * The KafkaConsumer instance which does the heavy lifting of
     * dealing with Kafka
     */
    private final KafkaConsumer<String, NabuCommand> consumer;

    /**
     * The current batch size, as adjusted based on performance
     */
    private final AtomicInteger currentBatchSize;

    /**
     * Whether or not the consumer should stop consuming as soon as it gets the chance
     */
    private final AtomicBoolean isStopped;

    /**
     * The actual Thread which performs Kafka consumption
     */
    private final Thread consumerThread;

    /**
     * A human-readable indentifier for this instance of SingleTPConsumer
     */
    private final String friendlyTPString;

    /**
     * The actual TopicPartition we will work against.
     * This is created via <code>new TopicPartition(throttlePolicy.getTopicName(), partitionToSubscribe);</code>
     */
    private final TopicPartition targetTopicPartition;

    /**
     * The minimum time to flush this next batch
     */
    private final AtomicLong nextFlushTime;


    public SingleTPConsumer(ESClient esClient,
                            ThrottlePolicy throttlePolicy,
                            int partitionToSubscribe,
                            KafkaBrokerConfigProvider config) {
        this.esClient = esClient;
        this.throttlePolicy = throttlePolicy;
        this.partitionToSubscribe = partitionToSubscribe;
        this.targetTopicPartition = new TopicPartition(throttlePolicy.getTopicName(), partitionToSubscribe);

        this.currentBatchSize = new AtomicInteger(throttlePolicy.getMaxBatchSize());
        this.isStopped = new AtomicBoolean(false);
        this.nextFlushTime = new AtomicLong(System.currentTimeMillis());

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", Joiner.on(',').join(config.getKafkaBrokers()));
        consumerProps.put("group.id", config.getKafkaGroup());
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("key.deserializer", StringDeserializer.class.getCanonicalName());
        consumerProps.put("value.deserializer", NabuCommandKafkaDeserializer.class.getCanonicalName());

        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumerThread = new Thread(this::runConsumer);
        this.friendlyTPString = String.format("SingleTPConsumer(%s[%d])", throttlePolicy.getIndexName(), partitionToSubscribe);
        this.consumerThread.setName(this.friendlyTPString);
    }

    private void runConsumer() {
        try {
            consumer.assign(ImmutableList.of(targetTopicPartition));
            List<NabuCommand> consumptionQueue = Lists.newArrayListWithExpectedSize(throttlePolicy.getMaxBatchSize());
            long lastConsumedOffset;
            while(!isStopped.get()) {
                ConsumerRecords<String, NabuCommand> thisPass = consumer.poll(300);
                int consumed = 0;
                lastConsumedOffset = -1;
                int currentBatchLimit = currentBatchSize.get();
                for(ConsumerRecord<String, NabuCommand> oneRecord : thisPass) {
                    consumed++;
                    consumptionQueue.add(oneRecord.value());
                    lastConsumedOffset = oneRecord.offset();

                    if(consumed == currentBatchLimit || System.currentTimeMillis() >= nextFlushTime.get()) {
                        writeAndFlush(consumptionQueue, lastConsumedOffset);
                        consumed = 0;
                    }
                }
                // we ran out of records on this poll loop, flush anything that's left, if necessary
                writeAndFlush(consumptionQueue, lastConsumedOffset);
            }
        } catch(WakeupException e) {
            if(!isStopped.get()) {
                throw e;
            }
        } finally {
            this.consumer.close();
        }
    }

    /**
     * Processes consumptionQueue by calling {@link SingleTPConsumer#performWrite(List)}, and commits lastConsumedOffset to Kafka
     * @param consumptionQueue the list of records to consume
     * @param lastConsumedOffset the offset of the last record in the consumptionQueue
     */
    private void writeAndFlush(List<NabuCommand> consumptionQueue, long lastConsumedOffset) {
        if(consumptionQueue.size() != 0) {
            performWrite(consumptionQueue);
            consumer.commitSync(ImmutableMap.of(targetTopicPartition, new OffsetAndMetadata(lastConsumedOffset)));
            consumptionQueue.clear();
        }
    }

    private void performWrite(List<NabuCommand> consumptionQueue) {
        Client realClient = esClient.getESClient();
        BulkRequestBuilder brb = realClient.prepareBulk();

        for(NabuCommand c : consumptionQueue) {
            if(c instanceof IndexCommand) {
                IndexCommand ic = ((IndexCommand)c);
                IndexRequestBuilder irb = realClient.prepareIndex();

                irb.setIndex(ic.getIndex())
                   .setSource(ic.getDocumentSource())
                   .setType(ic.getDocumentType())
                   .setRefresh(ic.shouldRefresh());

                brb.add(irb);
            } else if(c instanceof UpdateCommand) {
                UpdateCommand uc = ((UpdateCommand)c);
                UpdateRequestBuilder urb = realClient.prepareUpdate();

                urb.setIndex(uc.getIndex())
                   .setId(uc.getDocID())
                   .setType(uc.getDocumentType())
                   .setRefresh(uc.shouldRefresh())
                   .setRetryOnConflict(5); // todo: configurable in ThrottlePolicy? or nah?

                if(uc.hasUpdateScript()) {
                    urb.setScript(new Script(
                            uc.getUpdateScript(),
                            ScriptService.ScriptType.INLINE,
                            null,
                            uc.getScriptParams()
                    ));

                    if(uc.hasUpsert()) {
                        urb.setUpsert(uc.getDocumentSource());
                    }
                } else {
                    urb.setDoc(uc.getDocumentSource());
                    if(uc.hasUpsert()) {
                        urb.setDocAsUpsert(true);
                    }
                }

                brb.add(urb);
            }
        }

        BulkResponse response = brb.execute().actionGet();
        long reqTime = response.getTookInMillis();
        logger.info("Bulk request took {} millis, and not a single fuck was given that day.", reqTime);
        // todo: adjust batch size based on this somehow?

        for(BulkItemResponse bir : response.getItems()) {
            if(bir.isFailed()) {
                logger.warn("[ES BULK WRITE FAILURE] {}/{}[{}] :: {}",
                        bir.getIndex(),
                        bir.getType(),
                        bir.getId(),
                        bir.getFailureMessage());
            }
        }
    }

    @Override
    public void start() throws ComponentException {
        logger.info("Starting {}", friendlyTPString);
        this.consumerThread.start();
        logger.info("Started {}", friendlyTPString);
    }

    @Override
    public void shutdown() throws ComponentException {
        logger.info("Stopping {}", friendlyTPString);
        this.isStopped.set(true);
        this.consumer.wakeup();

        boolean joined = false;
        int attempts = 1;
        while(!joined) {
            try {
                logger.info("Joining ConsumerThread as part of shutdown of {}, attempt {}/5", friendlyTPString, attempts);
                this.consumerThread.join();
                joined = true;
            } catch(InterruptedException e) {
                attempts += 1;
                if(attempts <= 5) {
                    logger.warn("Interrupted while trying to join() on {}", friendlyTPString);
                } else {
                    logger.error("5 attempts to join() on {} failed, stop()ing the Thread", friendlyTPString);
                    this.consumerThread.stop();
                }
            }
        }
        logger.info("Stopped {}", friendlyTPString);
    }
}
