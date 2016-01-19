package io.stat.nabuproject.nabu.kafka;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Queues;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.telemetry.TelemetryGaugeSink;
import io.stat.nabuproject.core.telemetry.TelemetryService;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.nabu.common.command.NabuWriteCommand;
import io.stat.nabuproject.nabu.elasticsearch.ESWriteResults;
import io.stat.nabuproject.nabu.elasticsearch.NabuCommandESWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A managed wrapper around a KafkaConsumer that consumes only a single instance
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
final class SingleTPConsumer extends Component {
    private static final long FLUSH_INTERVAL = 5000; // todo: remember that whole tunable thing? Pepperidge Farms remembers!
    private static final String WAKEUP_INSIDE_UNSTOPPED_CONSUMER_WHILE_WRITING =
            "A WakeupException was thrown INSIDE the consumer loop. A replay scenario may occur the next time " +
            "this TopicPartition is consumed.";
    private static final String WAKEUP_INSIDE_UNSTOPPED_CONSUMER_NOT_WRITING =
            "A WakeupException was thrown INSIDE the consumer loop. However, the consumer was not in the process of writing " +
                    "and a replay scenario is unlikely.";
    /**
     * The NabuCommand ES Writer we will be using to execute our logic.
     */
    private final NabuCommandESWriter esWriter;

    /**
     * The throttle policy that specifies target write time and max batch size
     */
    private final AtomicReference<ThrottlePolicy> throttlePolicy;

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
     * When shutting down, this thread starts and waits for wakeupLatch to trip.
     * The consumerThread checks whether the isStopped flag is set at the start of every
     * consumption round, and if it is, trips wakeupLatch and exits out of the consumer loop.
     */
    private final Thread consumerWakeupWait;

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

    /**
     * Used to wait for a WriteAndFlush operation to finish before waking the Kafka consumer
     * when shutting down. This ensures that the latest consumed offsets are sync'd to kafka to prevent
     * a double-write/double-update scenario.
     */
    private final CountDownLatch wakeupLatch;

    private final TelemetryGaugeSink batchSizeGauge;
    private final TelemetryGaugeSink writeTimeGauge;
    private final TelemetryGaugeSink targetTimeGauge;


    public SingleTPConsumer(NabuCommandESWriter esWriter,
                            AtomicReference<ThrottlePolicy> throttlePolicy,
                            int partitionToSubscribe,
                            KafkaBrokerConfigProvider config,
                            TelemetryService telemetryService) {
        this.esWriter = esWriter;
        this.throttlePolicy = throttlePolicy;
        this.targetTopicPartition = new TopicPartition(throttlePolicy.get().getTopicName(), partitionToSubscribe);

        this.currentBatchSize = new AtomicInteger(throttlePolicy.get().getMaxBatchSize());
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
        this.friendlyTPString = String.format("SingleTPConsumer(%s[%d])", throttlePolicy.get().getIndexName(), partitionToSubscribe);
        this.consumerThread.setName(this.friendlyTPString);

        this.readyLatch = new CountDownLatch(1);
        this.wakeupLatch = new CountDownLatch(1);

        this.consumerWakeupWait = new Thread(() -> {
            try {
                this.wakeupLatch.await();
            } catch (Exception e) {
                logger.error("An exception was thrown while waiting for the consumer to acknowledge its shutdown. " +
                        "You may have a replay scenario occur when this TopicPartition starts being consumed again.", e);

            }
        });
        this.consumerWakeupWait.setName(this.friendlyTPString + "-FinishConsumeWait");

        this.batchSizeGauge = telemetryService.createGauge("consumer.batchsize", "topic:"+throttlePolicy.get().getTopicName(), "partition:"+partitionToSubscribe);
        this.writeTimeGauge = telemetryService.createGauge("consumer.writetime", "topic:"+throttlePolicy.get().getTopicName(), "partition:"+partitionToSubscribe);
        this.targetTimeGauge = telemetryService.createGauge("consumer.target", "topic:"+throttlePolicy.get().getTopicName(), "partition:"+partitionToSubscribe);
    }

    private void runConsumer() {
        boolean isInWriteAndFlush = false;
        try {
            consumer.assign(ImmutableList.of(targetTopicPartition));
            this.readyLatch.countDown();
            ArrayDeque<ConsumerRecord<String,NabuWriteCommand>> consumptionBacklog = Queues.newArrayDeque();
            ArrayDeque<NabuWriteCommand> immediateConsumptionQueue = new ArrayDeque<>(throttlePolicy.get().getMaxBatchSize() * 4);
            long lastConsumedOffset;
            while(!isStopped.get() || !consumptionBacklog.isEmpty()) { // items in the backlog can preempt stopping the consumer, see explanation below
                int consumed = 0;
                int currentBatchLimit = currentBatchSize.get();
                long now = System.currentTimeMillis();
                long flushTimeout = nextFlushTime.get();
                long startOffset = Long.MIN_VALUE;   // todo: does Kafka reserve this for anything/should i use a bool?
                lastConsumedOffset = Long.MIN_VALUE; // todo: ditto

                boolean hadBacklog = !consumptionBacklog.isEmpty();
                int backlogConsumed = 0;

                if(hadBacklog) {
                    startOffset = consumptionBacklog.peek().offset();
                    Iterator<ConsumerRecord<String, NabuWriteCommand>> backlogIterator = consumptionBacklog.iterator();
                    while(backlogIterator.hasNext() && backlogConsumed < currentBatchLimit) {
                        ConsumerRecord<String, NabuWriteCommand> cr = backlogIterator.next();
                        immediateConsumptionQueue.addLast(cr.value());
                        lastConsumedOffset = cr.offset();
                        backlogIterator.remove();
                        backlogConsumed++;
                    }

                    logger.info("Had backlog, consumed {}, {} remaining in backlog.", backlogConsumed, consumptionBacklog.size());
                }

                consumed += backlogConsumed;

                // if we cleared the backlog but still have room or time for more...
                // AND we're not stopping. this is important because if we ever want to reuse consumer objects,
                // we still want the loop to run and flush the backlog, but not grow it..
                while(consumed < currentBatchLimit && System.currentTimeMillis() < flushTimeout && !isStopped.get()) {
                    long pollTimeout = 200; // todo: figure out the math behind this and how it relates to nextFlushTime and targetTime
                    ConsumerRecords<String, NabuWriteCommand> thisPass = consumer.poll(pollTimeout);

                    if(startOffset == Long.MIN_VALUE && thisPass.count() > 0) {
                        startOffset = thisPass.records(targetTopicPartition).get(0).offset();
                    }

                    // add as many records as we can fit into the queue.
                    Iterator<ConsumerRecord<String, NabuWriteCommand>> thisPassIterator = thisPass.iterator();
                    while(thisPassIterator.hasNext() && consumed < currentBatchLimit) {
                        ConsumerRecord<String, NabuWriteCommand> cr = thisPassIterator.next();
                        immediateConsumptionQueue.addLast(cr.value());
                        lastConsumedOffset = cr.offset();
                        consumed++;
                    }

                    // put everything else into the backlog to be picked up again before the next poll.
                    if(thisPassIterator.hasNext()) {
                        while(thisPassIterator.hasNext()) {
                            consumptionBacklog.addLast(thisPassIterator.next());
                        }
                    }
                }

                if(consumed != 0) {
                    logger.info("Consumed {} with {} of it backlog", consumed, backlogConsumed);
                    isInWriteAndFlush = true;
                    writeCommitAndAdjust(immediateConsumptionQueue, startOffset, lastConsumedOffset);
                    immediateConsumptionQueue.clear();
                    isInWriteAndFlush = false;
                }

                // todo: adjust nextFlushTime here as needed
                nextFlushTime.set(System.currentTimeMillis() + FLUSH_INTERVAL);
            }
        } catch(WakeupException e) {
            String messageToUse = isInWriteAndFlush ? WAKEUP_INSIDE_UNSTOPPED_CONSUMER_WHILE_WRITING : WAKEUP_INSIDE_UNSTOPPED_CONSUMER_NOT_WRITING;
            if(!isStopped.get()) {
                logger.error(messageToUse, e);
            }
        } finally {
            this.consumer.close();
            this.wakeupLatch.countDown();
        }
    }

    /**
     * Processes consumptionQueue by delegating to the {@link SingleTPConsumer#esWriter}, and commits lastConsumedOffset to Kafka.
     * Then recomputes the batch size and next flush time as needed based on the performance of the write.
     * @param consumptionQueue the list of records to consume
     * @param lastConsumedOffset the offset of the last record in the consumptionQueue
     */
    private void writeCommitAndAdjust(ArrayDeque<NabuWriteCommand> consumptionQueue, long startedWithOffset, long lastConsumedOffset) {
        int qsize = consumptionQueue.size();
        if(qsize != 0) {
            ESWriteResults res = esWriter.bulkWrite(consumptionQueue);
            logger.debug("Bulk write: {}", res);
            consumer.commitSync(ImmutableMap.of(targetTopicPartition, new OffsetAndMetadata(lastConsumedOffset)));
            logger.info("{}=>{} ({} docs)", startedWithOffset, lastConsumedOffset, qsize);

            // todo: adjust currentBatchSize here based on res
            setCurrentBatchSize(throttlePolicy.get().getMaxBatchSize());
            writeTimeGauge.set(res.getTime() / 1000000); // lol nanos
            targetTimeGauge.set(throttlePolicy.get().getWriteTimeTarget());
        }
    }

    void setCurrentBatchSize(int newSize) {
        currentBatchSize.set(newSize);
        batchSizeGauge.set(newSize);
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

        this.consumerWakeupWait.start();

        try {
            this.consumerWakeupWait.join(120000); // a generous 2 minutes to allow one final poll, consume, flush and sync and exit.
        } catch (InterruptedException e) {
            logger.error("ConsumerWakeupWait was interrupted while trying to shut down the consumer loop.", e);
        } finally {
            this.consumer.wakeup();
        }

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
