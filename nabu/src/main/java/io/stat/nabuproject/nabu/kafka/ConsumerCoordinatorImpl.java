package io.stat.nabuproject.nabu.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.enkiprotocol.EnkiSourcedConfigKeys;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventSource;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAssign;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiConfigure;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiUnassign;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Listens for assignments by Enki to start consuming Kafka topics, and
 * creates instances of Consumers to do just that.
 */
@Slf4j
class ConsumerCoordinatorImpl extends AssignedConsumptionCoordinator {
    private final EnkiClientEventSource eces;
    private final ESClient esClient;
    private final Map<TopicPartition, SingleTPConsumer> consumerMap;
    private final byte[] $consumerMapLock;

    // todo: refactor sessionsourcedwhatever to hold these?
    private final AtomicReference<List<ThrottlePolicy>> throttlePolicies;
    private final AtomicReference<List<String>> kafkaBrokers;
    private final AtomicReference<String> kafkaGroup;

    private final byte[] $sourcedConfigLock;

    private final SessionSourcedKafkaBrokerCP sskbcp;

    @Inject
    ConsumerCoordinatorImpl(ESClient esClient, EnkiClientEventSource eces) {
        this.esClient = esClient;
        this.eces = eces;

        this.throttlePolicies = new AtomicReference<>(null);
        this.kafkaBrokers = new AtomicReference<>(null);
        this.kafkaGroup = new AtomicReference<>(null);
        this.$sourcedConfigLock = new byte[0];

        this.consumerMap = Maps.newConcurrentMap();
        this.$consumerMapLock = new byte[0];

        this.sskbcp = this.new SessionSourcedKafkaBrokerCP();
    }

    @Override
    public boolean onConfigurationReceived(EnkiConnection enki, EnkiConfigure packet) {
        synchronized ($consumerMapLock) {
            synchronized ($sourcedConfigLock) {
                if(throttlePolicies.get() != null) {
                    logger.error("Tried to reconfigure throttle policies in the same session. This is currently unsupported.");
                    return false;
                }

                if(kafkaBrokers.get() != null) {
                    logger.error("Tried to reconfigure Kafka brokers in the same session. This is currently unsupported.");
                    return false;
                }

                if(kafkaGroup.get() != null) {
                    logger.error("Tried to reconfigure Kafka group in the same session. This is currently unsupported.");
                    return false;
                }

                Map<String, Serializable> sourcedConfigs = packet.getOptions();

                //noinspection unchecked we all know what's really happening here...
                throttlePolicies.set((List<ThrottlePolicy>)
                        sourcedConfigs.getOrDefault(EnkiSourcedConfigKeys.THROTTLE_POLICIES, ImmutableList.of()));

                //noinspection unchecked ditto
                kafkaBrokers.set(
                    (List<String>) sourcedConfigs.getOrDefault(EnkiSourcedConfigKeys.KAFKA_BROKERS, ImmutableList.of())
                );

                kafkaGroup.set((String) sourcedConfigs.getOrDefault(EnkiSourcedConfigKeys.KAFKA_GROUP, ""));

                logger.info("Initialized session-sourced configs");
            }
        }
        return true;
    }

    @Override
    public boolean onTaskAssigned(EnkiConnection enki, EnkiAssign packet) {
        synchronized($sourcedConfigLock) {
            List<ThrottlePolicy> throttlePolicies = this.throttlePolicies.get();
            List<String> kafkaBrokers = this.kafkaBrokers.get();
            String kafkaGroup = this.kafkaGroup.get();

            if(throttlePolicies == null) {
                logger.error("Tried to get tasks assigned before throttle policies have been configured by Enki. This is morally abhorrent.");
                return false;
            }

            if(kafkaBrokers == null) {
                logger.error("Tried to get tasks assigned before Kafka brokers have been configured by Enki. This is morally abhorrent.");
                return false;
            }

            if(kafkaGroup == null) {
                logger.error("Tried to get tasks assigned before the Kafka group has been configured by Enki. This is morally abhorrent.");
                return false;
            }


            synchronized ($consumerMapLock) {
                TopicPartition tp = new TopicPartition(packet.getIndexName(), packet.getPartitionNumber());

                if(consumerMap.containsKey(tp)) {
                    logger.error("Tried to re-assign the same TopicPartition to this worker. This is beyond wrong.");
                    return false;
                } else {
                    ThrottlePolicy maybeTP = throttlePolicies.stream()
                                                             .filter((policy ->
                                                                 policy.getTopicName().equals(packet.getIndexName())
                                                             ))
                                                             .findFirst()
                                                             .orElse(null);

                    if(maybeTP == null) {
                        logger.error("Tried to assign a topic to consume for which this Nabu did not receive a ThrottlePolicy");
                        return false;
                    } else {
                        SingleTPConsumer stpc = new SingleTPConsumer(esClient, maybeTP, packet.getPartitionNumber(), sskbcp);

                        stpc.start();

                        consumerMap.put(tp, stpc);
                    }
                }
            }
            return true;
        }
    }

    @Override
    public boolean onTaskUnassigned(EnkiConnection enki, EnkiUnassign packet) {
        synchronized ($consumerMapLock) {
            TopicPartition tp = new TopicPartition(packet.getIndexName(), packet.getPartitionNumber());

            synchronized ($consumerMapLock) {
                if(!consumerMap.containsKey(tp)) {
                    logger.error("Tried to unassign a task that's never been assigned in the first place.");
                    return false;
                } else {
                    try {
                        consumerMap.get(tp).shutdown();
                    } catch(Exception e) {
                        logger.error("Something went wrong when trying to stop consumer for {}. Halting and catching fire.", e);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public boolean onConnectionLost(EnkiConnection enki, EnkiConnection.DisconnectCause reason, boolean wasAcked) {
        synchronized ($sourcedConfigLock) {
            kafkaGroup.set(null);
            kafkaBrokers.set(null);
            throttlePolicies.set(null);
        }
        return true;
    }

    @Override
    public void start() throws ComponentException {
        logger.info("Starting ConsumerCoordinatorImpl");
        eces.addEnkiClientEventListener(this);
        logger.info("Started ConsumerCoordinatorImpl");
    }

    @Override
    public void shutdown() throws ComponentException {
        logger.info("Stopping ConsumerCoordinatorImpl");
        eces.removeEnkiClientEventListener(this);

        synchronized ($consumerMapLock) {
            consumerMap.forEach((k, v) -> v.shutdown());
        }

        logger.info("Stopped ConsumerCoordinatorImpl");
    }

    // todo: probably extract this out to its own general sourcedconfigprovider.
    // which can be shared amongst both the consumers and other things that depend on
    // throttle policies and other things that are sourced from Enki.
    private class SessionSourcedKafkaBrokerCP implements KafkaBrokerConfigProvider {
        @Override
        public boolean isKafkaBrokerConfigAvailable() {
            return true;
        }

        @Override
        public List<String> getKafkaBrokers() {
            return ConsumerCoordinatorImpl.this.kafkaBrokers.get();
        }

        @Override
        public String getKafkaGroup() {
            return ConsumerCoordinatorImpl.this.kafkaGroup.get();
        }
    }
}
