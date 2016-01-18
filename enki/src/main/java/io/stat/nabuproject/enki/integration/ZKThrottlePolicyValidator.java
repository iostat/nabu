package io.stat.nabuproject.enki.integration;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.stat.nabuproject.core.DynamicComponent;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.zookeeper.ZKClient;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Validates that throttle policy config in ZK applies to our configuration,
 * and creates data if it doesn't already exist.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class ZKThrottlePolicyValidator {
    private final ZKClient zkClient;
    private final DynamicComponent<ThrottlePolicyProvider> dTPP;
    private final ZKThrottlePolicyProvider zkTPPToUse;

    @Inject
    ZKThrottlePolicyValidator(ZKClient zkClient,
                              DynamicComponent<ThrottlePolicyProvider> dTPP,
                              ZKThrottlePolicyProvider zkTPPToUse) {

        this.zkClient = zkClient;
        this.dTPP = dTPP;
        this.zkTPPToUse = zkTPPToUse;
    }

    boolean isValidToLead() {
        if(!dTPP.getClassOfDefaultInstance().isInstance(dTPP.getCurrentInstance())) {
            logger.error("The DynamicThrottlePolicyProvider is routing its requests to the default provider. This should never happen.");
            return false;
        }

        List<ThrottlePolicy> configuredPolicies = dTPP.getCurrentInstance().getThrottlePolicies();
        List<String> zkDefinedTPs = zkClient.getChildren(ZKThrottlePolicyProvider.TP_SUBNODE);

        if(zkDefinedTPs.size() == 0) {
            logger.info("No policies appear to exist in ZooKeeper, seeding them now.");
            initializeZKTPs(configuredPolicies);
            zkDefinedTPs = zkClient.getChildren(ZKThrottlePolicyProvider.TP_SUBNODE);
        } else {
            logger.info("Policies were found in ZooKeeper.");
        }

        if(configuredPolicies.size() != zkDefinedTPs.size()) {
            logger.error("The number of policies in ZooKeeper does not match the number of policies this Enki was configured with.");
            return false;
        }

        AtomicInteger matchedPolicies = new AtomicInteger(0);
        AtomicBoolean hadInvalidData = new AtomicBoolean(false);
        Map<String, String> seedData = Maps.newHashMap();
        zkDefinedTPs.forEach(zkTPName -> {
            String thisPolicyData = zkClient.get(zkTPPToUse.encodeTPPath(zkTPName));
            seedData.put(zkTPName, thisPolicyData);

            String[] thisPolicyRootBits = thisPolicyData.split(":");

            if(thisPolicyRootBits.length != 2) {
                logger.error("Policy {} had data {}, which does not match format <kafkaTopic>:<batchSize>,<targetTime>");
                hadInvalidData.set(true);
                return;
            }

            String kafkaTopicName = thisPolicyRootBits[0];
            String thisPolicyNumbers = thisPolicyRootBits[1];

            String[] thisPolicyThrottleBits = thisPolicyNumbers.split(",");

            int batchSize = 0;
            long tpWriteTime = 0;
            if(thisPolicyThrottleBits.length != 2) {
                logger.error("{} does not make sense as TP data (in node {})", thisPolicyThrottleBits, zkTPName);
                hadInvalidData.set(true);
            } else {
                try {
                    batchSize = Integer.parseInt(thisPolicyThrottleBits[0]);
                } catch(NumberFormatException e) {
                    logger.error("Could not parse {} as an integer. This is incorrect.", thisPolicyThrottleBits[0]);
                    hadInvalidData.set(true);
                    return;
                }

                if(batchSize <= 0) {
                    logger.error("Batch size {} does not make sense for throttle policy {}", batchSize, zkTPName);
                    hadInvalidData.set(true);
                    return;
                }

                try {
                    tpWriteTime = Long.parseLong(thisPolicyThrottleBits[1]);
                } catch(NumberFormatException e) {
                    logger.error("Could not parse target write time {} as a long. This is incorrect.", thisPolicyThrottleBits[1]);
                    hadInvalidData.set(true);
                    return;
                }

                if(tpWriteTime <= 0) {
                    logger.error("Write target {} does not make sense for throttle policy {}", tpWriteTime, zkTPName);
                    hadInvalidData.set(true);
                    return;
                }

                AtomicBoolean alreadyMatched = new AtomicBoolean(false);
                configuredPolicies.forEach(cp -> {
                    if(alreadyMatched.get()) { return; }
                    if(cp.getIndexName().equals(zkTPName)) {
                        if(cp.getTopicName().equals(kafkaTopicName)) {
                            matchedPolicies.incrementAndGet();
                        } else {
                            logger.error("Index {} with Kafka topic {} does not match ZK Kafka topic {}", zkTPName, kafkaTopicName, cp.getTopicName());
                            hadInvalidData.set(true);
                        }

                        alreadyMatched.set(true);
                    }
                });
            }
        });

        if(matchedPolicies.get() != configuredPolicies.size() || hadInvalidData.get()) {
            logger.error("One or more policies defined in ZooKeeper do not exist in this Enki's configuration or were malformed in ZK.");
            return false;
        }

        logger.info("Everything looks good, setting ZKThrottlePolicyProvider as the default one.");
        zkTPPToUse.seedTPs(seedData);
        dTPP.replaceInstance(zkTPPToUse);
        return true;
    }

    private void initializeZKTPs(List<ThrottlePolicy> allPolicies) {
        allPolicies.forEach(policy -> {
            String tpName = policy.getIndexName();
            String tpKafkaName = policy.getTopicName();
            int tpBatchSize = policy.getMaxBatchSize();
            long tpWriteTime = policy.getWriteTimeTarget();

            String nodePrefix = ZKThrottlePolicyProvider.TP_SUBNODE + "/" + tpName;

            zkClient.create(nodePrefix, ZKThrottlePolicyProvider.ENCODE_ZKTP(tpKafkaName, tpBatchSize, tpWriteTime));

            logger.info("Seeded TP index:{} with topic:{},  batch size:{} records and goal write:{} ms", tpName, tpKafkaName, tpBatchSize, tpWriteTime);
        });
    }
}
