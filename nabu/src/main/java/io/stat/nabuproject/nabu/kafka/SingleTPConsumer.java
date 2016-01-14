package io.stat.nabuproject.nabu.kafka;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.nabu.common.command.NabuWriteCommand;
import io.stat.nabuproject.nabu.elasticsearch.NabuCommandESWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
     * The NabuCommand ES Writer we will be using to execute our logic.
     */
    private final NabuCommandESWriter esWriter;

    /**
     * The throttle policy that specifies target write time and max batch size
     */
    private final ThrottlePolicy throttlePolicy;

    /**
     * The KafkaConsumer instance which does the heavy lifting of
     * dealing with Kafka
     */
    private final KafkaConsumer<String, NabuWriteCommand> consumer;

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

    /**
     * Used to make starting this consumer synchronous
     */
    private final CountDownLatch readyLatch;


    public SingleTPConsumer(NabuCommandESWriter esWriter,
                            ThrottlePolicy throttlePolicy,
                            int partitionToSubscribe,
                            KafkaBrokerConfigProvider config) {
        this.esWriter = esWriter;
        this.throttlePolicy = throttlePolicy;
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

        this.readyLatch = new CountDownLatch(1);
    }

    private void runConsumer() {
        try {
            consumer.assign(ImmutableList.of(targetTopicPartition));
            this.readyLatch.countDown();
            List<NabuWriteCommand> consumptionQueue = Lists.newArrayListWithExpectedSize(throttlePolicy.getMaxBatchSize());
            long lastConsumedOffset;
            while(!isStopped.get()) {
                ConsumerRecords<String, NabuWriteCommand> thisPass = consumer.poll(300);
                int consumed = 0;
                lastConsumedOffset = -1;
                int currentBatchLimit = currentBatchSize.get();
                for(ConsumerRecord<String, NabuWriteCommand> oneRecord : thisPass) {
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
    private void writeAndFlush(List<NabuWriteCommand> consumptionQueue, long lastConsumedOffset) {
        if(consumptionQueue.size() != 0) {
            performWrite(consumptionQueue);
            consumer.commitSync(ImmutableMap.of(targetTopicPartition, new OffsetAndMetadata(lastConsumedOffset)));
            consumptionQueue.clear();
        }
    }

    private void performWrite(List<NabuWriteCommand> consumptionQueue) {
        long timeTaken = esWriter.bulkWrite(consumptionQueue);
        logger.info("Write took {} ms", timeTaken);
    }

    @Override
    public void start() throws ComponentException {
        this.consumerThread.start();
        try { this.readyLatch.await(1, TimeUnit.SECONDS); } catch(Exception e) { throw new ComponentException(e); }
        logger.info("Started {}", friendlyTPString);
    }

    @Override
    public void shutdown() throws ComponentException {
        this.isStopped.set(true);
        this.consumer.wakeup();

        boolean joined = false;
        int attempts = 1;
        while(!joined) {
            try {
                logger.debug("Joining ConsumerThread as part of shutdown of {}, attempt {}/5", friendlyTPString, attempts);
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
