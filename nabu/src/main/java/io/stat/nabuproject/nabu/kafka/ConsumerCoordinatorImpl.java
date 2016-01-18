package io.stat.nabuproject.nabu.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.enkiprotocol.EnkiSourcedConfigKeys;
import io.stat.nabuproject.core.enkiprotocol.client.EnkiConnection;
import io.stat.nabuproject.core.enkiprotocol.dispatch.EnkiClientEventSource;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiAssign;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiConfigure;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiUnassign;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.telemetry.TelemetryCounterSink;
import io.stat.nabuproject.core.telemetry.TelemetryService;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.nabu.elasticsearch.NabuCommandESWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Listens for assignments by Enki to start consuming Kafka topics, and
 * creates instances of Consumers to do just that.
 */
@Slf4j
class ConsumerCoordinatorImpl extends AssignedConsumptionCoordinator {
    private final EnkiClientEventSource eces;
    private final TelemetryService telemetryService;
    private final NabuCommandESWriter esWriter;
    private final Map<TopicPartition, SingleTPConsumer> consumerMap;
    private final byte[] $consumerMapLock;

    // todo: refactor sessionsourcedwhatever to hold these?
    private final AtomicReference<List<AtomicReference<ThrottlePolicy>>> throttlePolicies;
    private final AtomicReference<List<String>> kafkaBrokers;
    private final AtomicReference<String> kafkaGroup;

    private final AtomicBoolean isShuttingDown;

    private final byte[] $sourcedConfigLock;
    private final SessionSourcedKafkaBrokerCP sskbcp;
    private final AtomicBoolean configurationSetAtLeastOnce;

    private final TelemetryCounterSink startedConsumerCounter;
    private final TelemetryCounterSink stoppedConsumerCounter;
    private final TelemetryCounterSink activeConsumerCounter;

    @Inject
    ConsumerCoordinatorImpl(NabuCommandESWriter esWriter,
                            EnkiClientEventSource eces,
                            TelemetryService telemetryService) {
        this.esWriter = esWriter;
        this.eces = eces;
        this.telemetryService = telemetryService;

        this.throttlePolicies = new AtomicReference<>(Lists.newArrayList());
        this.kafkaBrokers = new AtomicReference<>(null);
        this.kafkaGroup = new AtomicReference<>(null);
        this.$sourcedConfigLock = new byte[0];

        this.consumerMap = Maps.newHashMap();
        this.$consumerMapLock = new byte[0];

        this.configurationSetAtLeastOnce = new AtomicBoolean(false);
        this.isShuttingDown = new AtomicBoolean(false);

        this.sskbcp = this.new SessionSourcedKafkaBrokerCP();

        this.startedConsumerCounter = telemetryService.createCounter("consumers.started");
        this.stoppedConsumerCounter = telemetryService.createCounter("consumers.stopped");
        this.activeConsumerCounter  = telemetryService.createCounter("consumers.active");
    }

    @Override
    public boolean onConfigurationReceived(EnkiConnection enki, EnkiConfigure packet) {
        synchronized ($sourcedConfigLock) {
            Map<String, Serializable> sourcedConfigs = packet.getOptions();

            if(throttlePolicies.get() != null) {
                logger.error("Trying to reconfigure throttle policies in the same session. This is shaky at best");
            }

            //noinspection unchecked we all know what's really happening here...
            List<ThrottlePolicy> newList = ((List<ThrottlePolicy>) sourcedConfigs.getOrDefault(EnkiSourcedConfigKeys.THROTTLE_POLICIES, ImmutableList.of()));
            ThrottlePolicyProvider.performTPMerge(newList, throttlePolicies.get(), true, logger);

            if(kafkaBrokers.get() != null) {
                logger.warn("Reconfiguring Kafka brokers is unsupported and this consumer coordinator will not update them");
            } else {
                //noinspection unchecked ditto
                kafkaBrokers.set(
                        (List<String>) sourcedConfigs.getOrDefault(EnkiSourcedConfigKeys.KAFKA_BROKERS, ImmutableList.of())
                );
            }

            if(kafkaGroup.get() != null) {
                logger.warn("Reconfiguring Kafka group is unsupported and this consumer coordinator will not update them");
            } else {
                kafkaGroup.set((String) sourcedConfigs.getOrDefault(EnkiSourcedConfigKeys.KAFKA_GROUP, ""));
            }

            configurationSetAtLeastOnce.set(true);

            logger.info("Initialized session-sourced configs");
        }
        return true;
    }

    @Override
    public boolean onTaskAssigned(EnkiConnection enki, EnkiAssign packet) {
        synchronized($sourcedConfigLock) {
            List<AtomicReference<ThrottlePolicy>> throttlePolicies = this.throttlePolicies.get();
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
                    AtomicReference<ThrottlePolicy> maybeTP = throttlePolicies.stream()
                                                             .filter((policy ->
                                                                 policy.get().getTopicName().equals(packet.getIndexName())
                                                             ))
                                                             .findFirst()
                                                             .orElse(null);

                    if(maybeTP == null) {
                        logger.error("Tried to assign a topic to consume for which this Nabu did not receive a ThrottlePolicy");
                        return false;
                    } else {
                        SingleTPConsumer stpc = new SingleTPConsumer(esWriter, maybeTP, packet.getPartitionNumber(), sskbcp, telemetryService);
                        stpc.start();
                        startedConsumerCounter.increment();
                        activeConsumerCounter.increment();
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
            if(!consumerMap.containsKey(tp)) {
                logger.error("Tried to unassign a task that's never been assigned in the first place.");
                return false;
            } else {
                try {
                    consumerMap.get(tp).shutdown();
                    consumerMap.remove(tp);
                } catch(Exception e) {
                    logger.error("Something went wrong when trying to stop consumer for {}. Halting and catching fire.", e);
                    return false;
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

            synchronized ($consumerMapLock) {
                for(SingleTPConsumer tpc : consumerMap.values()) {
                    tpc.shutdown();
                }
                consumerMap.clear();
            }
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
        this.isShuttingDown.set(true);
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
            return ConsumerCoordinatorImpl.this.configurationSetAtLeastOnce.get();
        }

        @Override
        public boolean canEventuallyProvideConfig() {
            return !ConsumerCoordinatorImpl.this.isShuttingDown.get();
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
