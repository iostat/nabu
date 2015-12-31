package io.stat.nabuproject.core.kafka;

import java.util.List;

/**
 * Provides configuration to the ES module
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface KafkaZkConfigProvider {
    List<String> getKafkaZookeepers();
    String getKafkaZkChroot();
    int getKafkaZkConnTimeout();
    long getKafkaZkSessTimeout();
}
