package io.stat.nabuproject.enki;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.config.AbstractConfig;
import io.stat.nabuproject.core.config.ConfigStore;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.kafka.KafkaZkConfigProvider;
import io.stat.nabuproject.core.net.NetworkServerConfigProvider;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.leader.ZKLeaderConfigProvider;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * Adapter for all Enki-related configuration that is specified from
 * a config file.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Singleton @Slf4j
final class EnkiConfig extends AbstractConfig implements
        KafkaZkConfigProvider,
        ThrottlePolicyProvider,
        KafkaBrokerConfigProvider,
        ZKLeaderConfigProvider,
        NetworkServerConfigProvider {
    /**
     * Mapped to the enki.env property
     */
    private final @Getter String env;

    /**
     * Mapped to the enki.server.bind property
     */
    private final @Getter String listenAddress;

    /**
     * Mapped to the enki.server.port property
     */
    private final @Getter int listenPort;

    /**
     * Mapped to the enki.server.threads.acceptor property
     */
    private final @Getter int acceptorThreads;
    /**
     * Mapped to the enki.server.threads.worker property
     */
    private final @Getter int workerThreads;

    /**
     * Mapped to the enki.es.path.home property
     */
    private final @Getter(onMethod=@__(@Override)) String eSHome;

    /**
     * Mapped to the enki.es.cluster.name property
     */
    private final @Getter(onMethod=@__(@Override)) String eSClusterName;

    /**
     * Mapped to the enki.es.http.enabled property
     */
    private final @Getter(onMethod=@__(@Override)) boolean eSHTTPEnabled;

    /**
     * Mapped to the enki.es.http.port property
     */
    private final @Getter(onMethod=@__(@Override)) int eSHTTPPort;

    /**
     * Mapped to the enki.kafka.brokers property
     */
    private final @Getter List<String> kafkaBrokers;

    /**
     * Mapped to the enki.kafka.group property.
     */
    private final @Getter String kafkaGroup;

    /**
     * A list of Zookeeper servers that support the Kafka instances.
     * This is necessary for administrative purposes.
     */
    private final @Getter List<String> kafkaZookeepers;

    /**
     * Where the Kafka instance is chrooted under, if it is.
     */
    private final @Getter String kafkaZkChroot;

    /**
     * How long to wait before a Zk connection attempt is considered a timeout.
     */
    private final @Getter int kafkaZkConnTimeout;

    /**
     * Mapped to the enki.throttle.policies property.
     */
    private final @Getter List<ThrottlePolicy> throttlePolicies;

    private final @Getter List<String> lEZooKeepers;
    private final @Getter String lEZKChroot;
    private final @Getter int lEZKConnTimeout;

    @Inject
    public EnkiConfig(ConfigStore provider) {
        super(provider);

        this.env    = getRequiredProperty(Keys.ENKI_ENV, String.class);

        this.eSHome = getRequiredProperty(Keys.ENKI_ES_PATH_HOME, String.class);
        this.eSClusterName = getRequiredProperty(Keys.ENKI_ES_CLUSTER_NAME, String.class);
        this.eSHTTPEnabled = getOptionalProperty(Keys.ENKI_ES_HTTP_ENABLED, Defaults.ENKI_ES_HTTP_ENABLED, Boolean.class);
        this.eSHTTPPort    = getOptionalProperty(Keys.ENKI_ES_HTTP_PORT, Defaults.ENKI_ES_HTTP_PORT, Integer.class);

        this.kafkaBrokers  = getRequiredSequence(Keys.ENKI_KAFKA_BROKERS, String.class);
        this.kafkaGroup    = getOptionalProperty(Keys.ENKI_KAFKA_GROUP, "nabu_" + this.eSClusterName, String.class);

        this.kafkaZookeepers = getRequiredSequence(Keys.ENKI_KAFKA_ZK_SERVERS, String.class);
        this.kafkaZkChroot   = getOptionalProperty(Keys.ENKI_KAFKA_ZK_CHROOT, Defaults.ENKI_KAFKA_ZK_CHROOT, String.class);
        this.kafkaZkConnTimeout  = getOptionalProperty(Keys.ENKI_KAFKA_ZK_TIMEOUT, Defaults.ENKI_KAFKA_ZK_TIMEOUT, Integer.class);

        this.lEZooKeepers = getRequiredSequence(Keys.ENKI_LEADER_ZOOKEEPERS, String.class);
        this.lEZKChroot   = getRequiredProperty(Keys.ENKI_LEADER_ZKCHROOT, String.class);
        this.lEZKConnTimeout  = getOptionalProperty(Keys.ENKI_LEADER_ZKTIMEOUT, Defaults.ENKI_LEADER_ZKTIMEOUT, Integer.class);

        this.listenAddress = getOptionalProperty(Keys.ENKI_SERVER_BIND, Defaults.ENKI_SERVER_BIND, String.class);
        this.listenPort    = getOptionalProperty(Keys.ENKI_SERVER_PORT, Defaults.ENKI_SERVER_PORT, Integer.class);

        this.acceptorThreads = getOptionalProperty(Keys.ENKI_SERVER_THREADS_ACCEPTOR,
                Defaults.ENKI_SERVER_THREADS_ACCEPTOR,
                Integer.class);
        this.workerThreads   = getOptionalProperty(Keys.ENKI_SERVER_THREADS_WORKER,
                Defaults.ENKI_SERVER_THREADS_WORKER,
                Integer.class);

        this.throttlePolicies = getOptionalSequence(Keys.ENKI_THROTTLING_POLICIES, Defaults.ENKI_THROTTLING_POLICIES, ThrottlePolicy.class);
    }

    @Override
    public void start() throws ComponentException {
        StringBuilder prettyPolicies = new StringBuilder("The following throttling policies have been configured:\n");
        prettyPolicies.append(Strings.padStart("INDEX", 50, ' '));
        prettyPolicies.append(" MAXBATCH   TARGETMS\n");
        throttlePolicies.forEach(
                policy -> prettyPolicies.append(Strings.padStart(policy.getIndexName(), 50, ' '))
                        .append("    ")
                        .append(Strings.padEnd(Integer.toString(policy.getMaxBatchSize()), 7, ' '))
                        .append("    ")
                        .append(Strings.padEnd(Long.toString(policy.getWriteTimeTarget()), 7, ' '))
                        .append('\n')
        );
        logger.info(prettyPolicies.toString());
    }

    @Override
    public Map<String, String> getESNodeAttributes() {
        return ImmutableMap.of("enki", getListenAddress() + ":" + getListenPort());
    }

    @Override
    public boolean isKafkaBrokerConfigAvailable() {
        // always... always true...
        // it's only sourced from here
        return true;
    }

    @Override
    public void setKafkaBrokerConfig(List<String> brokers, String group) {
        String msg = "somebody tried to call EnkiConfig::setKafkaBrokerConfig. This is considered high treason.";
        logger.error(msg);
        throw new IllegalStateException(msg);
    }

    public static final class Keys {
        public static final String ENKI_ENV             = "enki.env";
        public static final String ENKI_ES_PATH_HOME    = "enki.es.path.home";
        public static final String ENKI_ES_CLUSTER_NAME = "enki.es.cluster.name";
        public static final String ENKI_ES_HTTP_ENABLED = "enki.es.http.enabled";
        public static final String ENKI_ES_HTTP_PORT    = "enki.es.http.port";

        public static final String ENKI_KAFKA_BROKERS    = "enki.kafka.brokers";
        public static final String ENKI_KAFKA_ZK_SERVERS = "enki.kafka.zk.servers";
        public static final String ENKI_KAFKA_ZK_CHROOT  = "enki.kafka.zk.chroot";
        public static final String ENKI_KAFKA_ZK_TIMEOUT = "enki.kafka.zk.timeout";
        public static final String ENKI_KAFKA_GROUP      = "enki.kafka.group";

        public static final String ENKI_LEADER_ZOOKEEPERS = "enki.leader.zookeepers";
        public static final String ENKI_LEADER_ZKCHROOT   = "enki.leader.zkchroot";
        public static final String ENKI_LEADER_ZKTIMEOUT  = "enki.leader.zktimeout";

        public static final String ENKI_SERVER_BIND             = "enki.server.bind";
        public static final String ENKI_SERVER_PORT             = "enki.server.port";
        public static final String ENKI_SERVER_THREADS_ACCEPTOR = "enki.server.threads.acceptor";
        public static final String ENKI_SERVER_THREADS_WORKER   = "enki.server.threads.worker";

        public static final String ENKI_THROTTLING_POLICIES = "enki.throttling.policies";
    }

    public static final class Defaults {
        public static final boolean ENKI_ES_HTTP_ENABLED = false;
        public static final int     ENKI_ES_HTTP_PORT    = 19216;

        public static final String ENKI_KAFKA_ZK_CHROOT  = "/";
        public static final int    ENKI_KAFKA_ZK_TIMEOUT = 500;

        public static final int    ENKI_LEADER_ZKTIMEOUT = 500;

        public static final String  ENKI_SERVER_BIND             = "0.0.0.0";
        public static final int     ENKI_SERVER_PORT             = 3654;
        public static final int     ENKI_SERVER_THREADS_ACCEPTOR = 1;
        public static final int     ENKI_SERVER_THREADS_WORKER   = 50;

        public static final List<ThrottlePolicy> ENKI_THROTTLING_POLICIES = ImmutableList.of();
    }
}
