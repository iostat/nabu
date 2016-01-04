package io.stat.nabuproject.core.kafka;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;

import java.util.Iterator;

/**
 * Provides a method of querying Kafka metadata using Zookeeper.
 * (The Kafka client APIs all have the chance of creating the topic if
 * it doesn't already exists, where as this uses Kafka's own AdminUtils)
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
final class KafkaMetadataClientImpl extends KafkaMetadataClient {
    private final KafkaZkConfigProvider config;
    private ZkClient zkClient;
    private final Object[] $zkClientLock;

    @Inject
    public KafkaMetadataClientImpl(KafkaZkConfigProvider config) {
        this.config = config;
        this.$zkClientLock = new Object[0];
    }

    @Override
    public void start() throws ComponentException {
        Iterator<String> chrootedZookeepersIterator =
                config.getKafkaZookeepers()
                      .stream()
                      .map(zk -> zk + config.getKafkaZkChroot())
                      .iterator();

        this.zkClient = new ZkClient(
                Joiner.on(',').join(chrootedZookeepersIterator),
                config.getKafkaZkConnTimeout());

        this.zkClient.setZkSerializer(new KafkaZKStringSerializerProxy());
    }

    @Override
    public void shutdown() throws ComponentException {
        if(this.zkClient != null) {
            this.zkClient.close();
        }
    }

    private ZkClient getZkClient() {
        if(!wasStarted() || wasStopped()) {
            throw new IllegalStateException("Attempted to call KafkaMetadataClientImpl::getZkClient in a stopped state!");
        } else {
            return zkClient;
        }
    }

    @Override
    public boolean topicExists(String topicName) {
        synchronized ($zkClientLock) {
            return AdminUtils.topicExists(getZkClient(), topicName);
        }
    }

    @Override
    public int topicPartitionsCount(String topicName) {
        synchronized ($zkClientLock) {
            TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicName, getZkClient());
            return topicMetadata.partitionsMetadata().size();
        }
    }
}
