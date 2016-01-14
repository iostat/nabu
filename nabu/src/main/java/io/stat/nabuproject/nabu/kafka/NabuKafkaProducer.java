package io.stat.nabuproject.nabu.kafka;

import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.nabu.common.command.NabuWriteCommand;
import io.stat.nabuproject.nabu.server.NabuCommandSource;

/**
 * Something which can take NabuWriteCommands and load them into Kafka.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class NabuKafkaProducer extends Component {
    /**
     * Write a command to the specified Kafka TopicPartition.
     * It is assumed that the caller of this method knows what the hell
     * theyre doing.
     * @param topic The Kafka topic to write to.
     * @param partition The partition of the topic to write to.
     * @param src the NabuCommandSource that received this command
     * @param command the command to write.
     * @return true if the write was successful, false otherwise.
     */
    public abstract boolean enqueueCommand(String topic, int partition, NabuCommandSource src, NabuWriteCommand command);
}
