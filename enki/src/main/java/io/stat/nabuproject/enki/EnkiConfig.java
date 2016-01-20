package io.stat.nabuproject.enki;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.config.AbstractConfig;
import io.stat.nabuproject.core.config.ConfigStore;
import io.stat.nabuproject.core.kafka.KafkaBrokerConfigProvider;
import io.stat.nabuproject.core.kafka.KafkaZkConfigProvider;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.net.AdvertisedAddressProvider;
import io.stat.nabuproject.core.net.NetworkServerConfigProvider;
import io.stat.nabuproject.core.telemetry.TelemetryConfigProvider;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.core.throttling.ThrottlePolicyChangeListener;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.enki.integration.WorkerCoordinatorConfigProvider;
import io.stat.nabuproject.enki.zookeeper.ZKConfigProvider;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Adapter for all Enki-related configuration that is specified from
 * a config file.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Singleton @Slf4j
final class EnkiConfig extends AbstractConfig implements
        KafkaZkConfigProvider,
        KafkaBrokerConfigProvider,
        ZKConfigProvider,
        NetworkServerConfigProvider,
        ThrottlePolicyProvider,
        TelemetryConfigProvider,
        WorkerCoordinatorConfigProvider,
        LeaderElectionTuningProvider {
    /**
     * Mapped to the enki.env property
     */
    private final @Getter String env;

    /**
     * Mapped to the enki.server.bind and enki.server.port propertoes
     */
    private final @Getter AddressPort listenBinding;

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
    private final @Getter List<AtomicReference<ThrottlePolicy>> tPReferences;
    private final @Getter List<String> zookeepers;
    private final @Getter String zKChroot;
    private final @Getter int zKConnectionTimeout;
    private final @Getter Map<String, String> additionalESProperties;

    private final @Getter String telemetryPrefix;
    private final @Getter String telemetryServer;
    private final @Getter int telemetryPort;

    private final @Getter long rebalancePeriod;
    private final @Getter long rebalanceKillTimeout;

    private final @Getter long settleDelay;
    private final @Getter long zKReconcileDelay;
    private final @Getter long noLeaderRetryDelay;
    private final @Getter long maxLeaderRetries;
    private final @Getter long eSEventSyncDelay;
    private final @Getter long eSEventSyncDelayGap;
    private final @Getter long maxDemotionFailsafeTimeout;

    private final Injector injector;

    private final AtomicReference<Collection<ThrottlePolicyChangeListener>> tpcls;

    @Inject
    public EnkiConfig(ConfigStore provider, Injector injector) {
        super(provider);
        this.injector = injector;

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

        this.zookeepers = getRequiredSequence(Keys.ENKI_ZK_ZOOKEEPERS, String.class);
        this.zKChroot = getRequiredProperty(Keys.ENKI_ZK_CHROOT, String.class);
        this.zKConnectionTimeout = getOptionalProperty(Keys.ENKI_ZK_TIMEOUT, Defaults.ENKI_ZK_TIMEOUT, Integer.class);

        String listenAddress = getOptionalProperty(Keys.ENKI_SERVER_BIND, Defaults.ENKI_SERVER_BIND, String.class);
        int    listenPort    = getOptionalProperty(Keys.ENKI_SERVER_PORT, Defaults.ENKI_SERVER_PORT, Integer.class);
        this.listenBinding = new AddressPort(listenAddress, listenPort);

        this.acceptorThreads = getOptionalProperty(Keys.ENKI_SERVER_THREADS_ACCEPTOR,
                Defaults.ENKI_SERVER_THREADS_ACCEPTOR,
                Integer.class);
        this.workerThreads   = getOptionalProperty(Keys.ENKI_SERVER_THREADS_WORKER,
                Defaults.ENKI_SERVER_THREADS_WORKER,
                Integer.class);

        List<ThrottlePolicy> loadedTPs = getOptionalSequence(Keys.ENKI_THROTTLING_POLICIES, Defaults.ENKI_THROTTLING_POLICIES, ThrottlePolicy.class);
        ImmutableList.Builder<AtomicReference<ThrottlePolicy>> tprefbuilder = ImmutableList.builder();
        for (ThrottlePolicy loadedTP : loadedTPs) {
            tprefbuilder.add(new AtomicReference<>(loadedTP));
        }

        this.tPReferences = tprefbuilder.build();

        this.tpcls = new AtomicReference<>(Lists.newArrayList());

        //noinspection unchecked todo: i know this is ratchet to coerce like this, but fuck it; pre-alpha motherfuckers!
        this.additionalESProperties = (Map)getOptionalSubmap(Keys.ENKI_ES_OTHER, ImmutableMap.of());

        this.telemetryPrefix = "enki";
        this.telemetryServer = getRequiredProperty(Keys.ENKI_TELEMETRY_SERVER, String.class);
        this.telemetryPort   = getRequiredProperty(Keys.ENKI_TELEMETRY_PORT, Integer.class);

        this.rebalancePeriod = getOptionalProperty(Keys.ENKI_COORD_REBALANCE, Defaults.ENKI_COORD_REBALANCE, Long.class);
        this.rebalanceKillTimeout = getOptionalProperty(Keys.ENKI_COORD_GRACE, Defaults.ENKI_COORD_GRACE, Long.class);

        this.settleDelay = getOptionalProperty(Keys.ENKI_LEADER_SETTLE_DELAY, Defaults.ENKI_LEADER_SETTLE_DELAY, Long.class);
        this.zKReconcileDelay = getOptionalProperty(Keys.ENKI_LEADER_RECONCILE_DELAY, Defaults.ENKI_LEADER_RECONCILE_DELAY, Long.class);
        this.noLeaderRetryDelay = getOptionalProperty(Keys.ENKI_LEADER_RETRY_DELAY, Defaults.ENKI_LEADER_RETRY_DELAY, Long.class);
        this.maxLeaderRetries = getOptionalProperty(Keys.ENKI_LEADER_MAX_RETRIES, Defaults.ENKI_LEADER_MAX_RETRIES, Long.class);
        this.eSEventSyncDelay = getOptionalProperty(Keys.ENKI_LEADER_ES_SYNC_DELAY, Defaults.ENKI_LEADER_ES_SYNC_DELAY, Long.class);
        this.eSEventSyncDelayGap = getOptionalProperty(Keys.ENKI_LEADER_ES_SYNC_GAP, Defaults.ENKI_LEADER_ES_SYNC_GAP, Long.class);
        this.maxDemotionFailsafeTimeout = getOptionalProperty(Keys.ENKI_LEADER_MAX_DEMOTION_FAILSAFE_TIMEOUT, Defaults.ENKI_LEADER_MAX_DEMOTION_FAILSAFE_TIMEOUT, Long.class);
    }

    @Override
    public void start() throws ComponentException {
        StringBuilder prettyPolicies = new StringBuilder("The following throttling policies have been configured:\n");
        prettyPolicies.append(Strings.padStart("INDEX", 25, ' '));
        prettyPolicies.append(Strings.padStart("TOPIC", 25, ' '));
        prettyPolicies.append(" MAXBATCH   TARGETMS   FLUSHMS\n");
        tPReferences.forEach(
                policy -> prettyPolicies.append(Strings.padStart(policy.get().getIndexName(), 25, ' '))
                        .append(Strings.padStart(policy.get().getTopicName(), 25, ' '))
                        .append("    ")
                        .append(Strings.padEnd(Integer.toString(policy.get().getMaxBatchSize()), 7, ' '))
                        .append("    ")
                        .append(Strings.padEnd(Long.toString(policy.get().getWriteTimeTarget()), 7, ' '))
                        .append("   ")
                        .append(Strings.padEnd(Long.toString(policy.get().getFlushTimeout()), 5, ' '))
                        .append('\n')
        );
        logger.info(prettyPolicies.toString());
    }

    @Override
    public Map<String, String> getESNodeAttributes() {
        // todo: need a better workaround to getting advertised address than just doing this
        //       without creating a potential circular dependency during constructor-injection
        //       also this whole thing is a bit of a hack really...
        AddressPort ap = injector.getInstance(AdvertisedAddressProvider.class).getAdvertisedAddress();
        return ImmutableMap.of("enki", ap.getAddress() + ":" + ap.getPort());
    }

    @Override
    public boolean isKafkaBrokerConfigAvailable() {
        // always... always true...
        // it's only sourced from here
        return true;
    }

    @Override
    public boolean canEventuallyProvideConfig() {
        // will never lose config
        return true;
    }

    @Override
    public void setKafkaBrokerConfig(List<String> brokers, String group) {
        String msg = "somebody tried to call EnkiConfig::setKafkaBrokerConfig. This is considered high treason.";
        logger.error(msg);
        throw new IllegalStateException(msg);
    }

    @Override
    public Collection<ThrottlePolicyChangeListener> getAllTPCLs() {
        return tpcls.get();
    }

    @Override
    public void seedTPCLs(Collection<ThrottlePolicyChangeListener> tpcls) {
        this.tpcls.set(tpcls);
    }

    public static final class Keys {
        public static final String ENKI_ENV             = "enki.env";
        public static final String ENKI_ES_PATH_HOME    = "enki.es.path.home";
        public static final String ENKI_ES_CLUSTER_NAME = "enki.es.cluster.name";
        public static final String ENKI_ES_HTTP_ENABLED = "enki.es.http.enabled";
        public static final String ENKI_ES_HTTP_PORT    = "enki.es.http.port";
        public static final String ENKI_ES_OTHER        = "enki.es.other";

        public static final String ENKI_KAFKA_BROKERS    = "enki.kafka.brokers";
        public static final String ENKI_KAFKA_GROUP      = "enki.kafka.group";
        public static final String ENKI_KAFKA_ZK_SERVERS = "enki.kafka.zk.servers";
        public static final String ENKI_KAFKA_ZK_CHROOT  = "enki.kafka.zk.chroot";
        public static final String ENKI_KAFKA_ZK_TIMEOUT = "enki.kafka.zk.timeout";

        public static final String ENKI_ZK_ZOOKEEPERS = "enki.zk.zookeepers";
        public static final String ENKI_ZK_CHROOT = "enki.zk.chroot";
        public static final String ENKI_ZK_TIMEOUT = "enki.zk.timeout";

        public static final String ENKI_SERVER_BIND             = "enki.server.bind";
        public static final String ENKI_SERVER_PORT             = "enki.server.port";
        public static final String ENKI_SERVER_THREADS_ACCEPTOR = "enki.server.threads.acceptor";
        public static final String ENKI_SERVER_THREADS_WORKER   = "enki.server.threads.worker";

        public static final String ENKI_THROTTLING_POLICIES = "enki.throttling.policies";

        public static final String ENKI_TELEMETRY_SERVER = "enki.telemetry.server";
        public static final String ENKI_TELEMETRY_PORT   = "enki.telemetry.port";

        public static final String ENKI_COORD_REBALANCE = "enki.coord.rebalance";
        public static final String ENKI_COORD_GRACE     = "enki.coord.grace";

        public static final String ENKI_LEADER_SETTLE_DELAY                  = "enki.leader.settle_delay";
        public static final String ENKI_LEADER_RECONCILE_DELAY               = "enki.leader.reconcile_delay";
        public static final String ENKI_LEADER_RETRY_DELAY                   = "enki.leader.retry_delay";
        public static final String ENKI_LEADER_MAX_RETRIES                   = "enki.leader.max_retries";
        public static final String ENKI_LEADER_ES_SYNC_DELAY                 = "enki.leader.es_sync_delay";
        public static final String ENKI_LEADER_ES_SYNC_GAP                   = "enki.leader.es_sync_gap";
        public static final String ENKI_LEADER_MAX_DEMOTION_FAILSAFE_TIMEOUT = "enki.leader.max_demotion_failsafe_timeout";
    }

    public static final class Defaults {
        public static final boolean ENKI_ES_HTTP_ENABLED = false;
        public static final int     ENKI_ES_HTTP_PORT    = 19216;

        public static final String ENKI_KAFKA_ZK_CHROOT  = "/";
        public static final int    ENKI_KAFKA_ZK_TIMEOUT = 500;

        public static final int ENKI_ZK_TIMEOUT = 500;

        public static final String  ENKI_SERVER_BIND             = "0.0.0.0";
        public static final int     ENKI_SERVER_PORT             = 3654;
        public static final int     ENKI_SERVER_THREADS_ACCEPTOR = 1;
        public static final int     ENKI_SERVER_THREADS_WORKER   = 50;

        public static final List<ThrottlePolicy> ENKI_THROTTLING_POLICIES = ImmutableList.of();

        public static final long ENKI_COORD_REBALANCE = 5000;
        public static final long ENKI_COORD_GRACE     = 2000;

        public static final long ENKI_LEADER_SETTLE_DELAY                  = 1000;
        public static final long ENKI_LEADER_RECONCILE_DELAY               = 500;
        public static final long ENKI_LEADER_RETRY_DELAY                   = 2000;
        public static final long ENKI_LEADER_MAX_RETRIES                   = 10;
        public static final long ENKI_LEADER_ES_SYNC_DELAY                 = 750;
        public static final long ENKI_LEADER_ES_SYNC_GAP                   = 30;
        public static final long ENKI_LEADER_MAX_DEMOTION_FAILSAFE_TIMEOUT = 12000;
    }
}
