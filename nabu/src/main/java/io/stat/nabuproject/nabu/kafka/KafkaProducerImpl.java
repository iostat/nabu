package io.stat.nabuproject.nabu.kafka;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.nabu.common.command.NabuCommand;
import io.stat.nabuproject.nabu.common.command.NabuWriteCommand;
import io.stat.nabuproject.nabu.server.NabuCommandSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The implementation of the canonical NabuKafkaProducer.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
/*
 * todo: this needs to listen to changes to the enki connection state, and recreate the
 * todo: producer as needed. should also backlog writes for in-between connections.
 * todo: failing backlogged writes when nabu shuts down is irrelevant, since
 * todo: when the nabu client loses connectivity to the nabu server, it FAILs any outstanding promises
 */
@RequiredArgsConstructor(onConstructor=@__(@Inject))
@Slf4j
class KafkaProducerImpl extends NabuKafkaProducer {
    private final KafkaBrokerConfigProvider config;
    private KafkaProducer<String, NabuCommand> backingProducer;
    private final AtomicBoolean isReady = new AtomicBoolean(false);

    @Override
    public void start() throws ComponentException {
        logger.info("Waiting for broker configuration...");
        while(!config.isKafkaBrokerConfigAvailable()) {
            try {
                if(!config.canEventuallyProvideConfig()) {
                    throw new ComponentException(true, "The Kafka Broker Config Provider is in a state that it will never provide a config.");
                }
                logger.info("Waiting for broker configuration...");
                Thread.sleep(1000);
            } catch(Exception e) {
                throw new ComponentException(true, "NabuKafkaProducer was interrupted before it could get the broker config.", e);
            }
        }

        logger.info("Working with Kafka configuration provider {}\n{}\n{}", config.getClass().getCanonicalName(), config.getKafkaBrokers(), config.getKafkaGroup());

        Properties props = new Properties();
        props.put("bootstrap.servers", Joiner.on(',').join(config.getKafkaBrokers()));
        props.put("retries", 0); // todo: tunable? how many times to retry a write... could THEORETICALLY lead to duplicates.
        props.put("batch.size", "10000"); // todo: should be tunable. also note that batch.size is per-topic-per-partition.
        props.put("buffer.memory", 1073741824); // todo: TUNABLE. AS. FUCK., max heap to take up buffering writes. (currently 1GB)
        props.put("linger.ms", 20); // todo: up 2 ms wait to fill up a batch before flushing. should probably be tunable
        props.put("key.serializer", StringSerializer.class.getCanonicalName());
        props.put("value.serializer", NabuCommandKafkaSerializer.class.getCanonicalName());

        backingProducer = new KafkaProducer<>(props);
        logger.info("Created Kafka producer!");
    }

    @Override
    public void shutdown() throws ComponentException {
        backingProducer.close();
    }

    @Override
    public boolean enqueueCommand(String topic, int partition, NabuCommandSource src, NabuWriteCommand command) {
        backingProducer.send(
                new ProducerRecord<>(topic, partition, command.getIndex(), command),
                (metadata, exception) -> {
                    if(exception != null) {
                        logger.error("Exception thrown when trying to write command to Kafka", exception);
                        src.respond(command.failResponse(command.getDocumentID(), String.format("[KAFKA] Could not queue into Kafka: %s :: %s", exception.getClass().getCanonicalName(), exception.getMessage())));
                    } else {
                        src.respond(command.queuedResponse(command.getDocumentID()));
                    }
                });

        return true;
    }
}
