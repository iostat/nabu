package io.stat.nabuproject.enki;

import com.google.inject.Inject;
import io.stat.nabuproject.core.config.Config;
import io.stat.nabuproject.core.config.ConfigurationProvider;
import lombok.Getter;

import java.util.List;

/**
 * Created by io on 12/28/15. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class EnkiConfig extends Config {
    /**
     * Mapped to the nabu.env property
     */
    private final @Getter String env;

    /**
     * Mapped to the nabu.server.bind property
     */
    private final @Getter String listenAddress;

    /**
     * Mapped to the nabu.server.port property
     */
    private final @Getter int listenPort;

    /**
     * Mapped to the nabu.server.threads.acceptor property
     */
    private final @Getter int acceptorThreads;
    /**
     * Mapped to the nabup.server.threads.worker property
     */
    private final @Getter int workerThreads;

    /**
     * Mapped to the nabu.es.path.home property
     */
    private final @Getter String eSHome;

    /**
     * Mapped to the nabu.es.http.enabled property
     */
    private final @Getter boolean eSHTTPEnabled;

    /**
     * Mapped to the nabu.es.http.port property
     */
    private final @Getter int eSHTTPPort;

    /**
     * Mapped to the nabu.kafka.brokers property
     */
    private final @Getter
    List<String> kafkaBrokers;

    /**
     * Mapped to the nabuproject.es.cluster.name property
     */
    private final @Getter String eSClusterName;

    @Inject
    public EnkiConfig(ConfigurationProvider provider) {
        super(provider);

        this.env    = getRequiredProperty(Keys.NABU_ENV, String.class);
        this.eSHome = getRequiredProperty(Keys.NABU_ES_PATH_HOME, String.class);
        this.eSClusterName = getRequiredProperty(Keys.NABU_ES_CLUSTER_NAME, String.class);
        this.kafkaBrokers  = getRequiredSequence(Keys.NABU_KAFKA_BROKERS, String.class);

        this.eSHTTPEnabled = getOptionalProperty(Keys.NABU_ES_HTTP_ENABLED, Defaults.NABU_ES_HTTP_ENABLED, Boolean.class);
        this.eSHTTPPort    = getOptionalProperty(Keys.NABU_ES_HTTP_PORT, Defaults.NABU_ES_HTTP_PORT, Integer.class);

        this.listenAddress = getOptionalProperty(Keys.NABU_SERVER_BIND, Defaults.NABU_SERVER_BIND, String.class);
        this.listenPort    = getOptionalProperty(Keys.NABU_SERVER_PORT, Defaults.NABU_SERVER_PORT, Integer.class);

        this.acceptorThreads = getOptionalProperty(Keys.NABU_SERVER_THREADS_ACCEPTOR,
                Defaults.NABU_SERVER_THREADS_ACCEPTOR,
                Integer.class);
        this.workerThreads   = getOptionalProperty(Keys.NABU_SERVER_THREADS_WORKER,
                Defaults.NABU_SERVER_THREADS_WORKER,
                Integer.class);
    }

    private static final class Keys {
        public static final String NABU_ENV             = "nabu.env";
        public static final String NABU_ES_PATH_HOME    = "nabu.es.path.home";
        public static final String NABU_ES_CLUSTER_NAME = "nabu.es.cluster.name";
        public static final String NABU_KAFKA_BROKERS   = "nabu.kafka.brokers";

        public static final String NABU_ES_HTTP_ENABLED = "nabu.es.http.enabled";
        public static final String NABU_ES_HTTP_PORT    = "nabu.es.http.port";

        public static final String NABU_SERVER_BIND             = "nabu.server.bind";
        public static final String NABU_SERVER_PORT             = "nabu.server.port";
        public static final String NABU_SERVER_THREADS_ACCEPTOR = "nabu.server.threads.acceptor";
        public static final String NABU_SERVER_THREADS_WORKER   = "nabu.server.threads.worker";
    }

    private static final class Defaults {
        public static final boolean NABU_ES_HTTP_ENABLED = false;
        public static final int     NABU_ES_HTTP_PORT    = 19216;

        public static final String  NABU_SERVER_BIND             = "127.0.0.1";
        public static final int     NABU_SERVER_PORT             = 6228;
        public static final int     NABU_SERVER_THREADS_ACCEPTOR = 1;
        public static final int     NABU_SERVER_THREADS_WORKER   = 10;
    }
}
