package io.stat.nabuproject.core.kafka;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.List;
import java.util.stream.Collectors;

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
    private ZkUtils zkUtils;
    private ZkClient zkClient;
    private ZkConnection zkConnection;
    private final Object[] $zkClientLock;

    @Inject
    public KafkaMetadataClientImpl(KafkaZkConfigProvider config) {
        this.config = config;
        this.$zkClientLock = new Object[0];
    }

    @Override
    public void start() throws ComponentException {
        List<String> chrootedZookeepers =
                config.getKafkaZookeepers()
                      .stream()
                      .map(zk -> zk + config.getKafkaZkChroot())
                      .collect(Collectors.toList());

        this.zkConnection = new ZkConnection(Joiner.on(',').join(chrootedZookeepers));
        this.zkClient = new ZkClient(
                this.zkConnection,
                config.getKafkaZkConnTimeout());
        this.zkClient.setZkSerializer(new KafkaZKStringSerializerProxy());

        this.zkUtils = new ZkUtils(this.zkClient, this.zkConnection, false); // todo: yeah that false.. yeah... seriously...

        logger.info("MetadataImpl start() :: {}", chrootedZookeepers);
    }

    @Override
    public void shutdown() throws ComponentException {
        if(this.zkUtils != null) {
            this.zkUtils.close();
        }

        if(this.zkClient != null) {
            this.zkClient.close();
        }
    }

    private ZkUtils getZkUtils() {
        if(!wasStarted() || wasStopped()) {
            throw new IllegalStateException("Attempted to call KafkaMetadataClientImpl::getZkClient in a stopped state!");
        } else {
            return zkUtils;
        }
    }

    @Override
    public boolean topicExists(String topicName) {
        synchronized ($zkClientLock) {
            return AdminUtils.topicExists(getZkUtils(), topicName);
        }
    }

    @Override
    public int topicPartitionsCount(String topicName) {
        synchronized ($zkClientLock) {
            TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicName, getZkUtils());
            return topicMetadata.partitionsMetadata().size();
        }
    }
}
