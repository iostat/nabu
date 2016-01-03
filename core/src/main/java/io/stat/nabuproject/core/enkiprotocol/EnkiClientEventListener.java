package io.stat.nabuproject.core.enkiprotocol;

import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.Map;

/**
 * Something which can receive Enki protocol events.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface EnkiClientEventListener {
    void onConfigurationReceived(Map<String, Serializable> config);
    void onTaskAssigned(TopicPartition topicPartition);
    void onTaskUnassigned(TopicPartition topicPartition);
    void onConnectionLost(boolean wasLeaving, boolean serverInitiated, boolean wasAcked);
}
