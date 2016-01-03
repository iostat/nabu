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
    void onConfigurationReceived(EnkiConnection enki, Map<String, Serializable> config);
    void onTaskAssigned(EnkiConnection enki, TopicPartition topicPartition);
    void onTaskUnassigned(EnkiConnection enki, TopicPartition topicPartition);
    void onConnectionLost(EnkiConnection enki, boolean wasLeaving, boolean serverInitiated, boolean wasAcked);
}
