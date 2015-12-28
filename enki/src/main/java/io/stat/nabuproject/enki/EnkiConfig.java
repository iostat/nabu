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

    @Inject
    public EnkiConfig(ConfigurationProvider provider) {
        super(provider);

        this.env    = getRequiredProperty(Keys.ENKI_ENV, String.class);
        this.eSHome = getRequiredProperty(Keys.ENKI_ES_PATH_HOME, String.class);
        this.eSClusterName = getRequiredProperty(Keys.ENKI_ES_CLUSTER_NAME, String.class);
        this.kafkaBrokers  = getRequiredSequence(Keys.ENKI_KAFKA_BROKERS, String.class);

        this.eSHTTPEnabled = getOptionalProperty(Keys.ENKI_ES_HTTP_ENABLED, Defaults.ENKI_ES_HTTP_ENABLED, Boolean.class);
        this.eSHTTPPort    = getOptionalProperty(Keys.ENKI_ES_HTTP_PORT, Defaults.ENKI_ES_HTTP_PORT, Integer.class);

        this.listenAddress = getOptionalProperty(Keys.ENKI_SERVER_BIND, Defaults.ENKI_SERVER_BIND, String.class);
        this.listenPort    = getOptionalProperty(Keys.ENKI_SERVER_PORT, Defaults.ENKI_SERVER_PORT, Integer.class);

        this.acceptorThreads = getOptionalProperty(Keys.ENKI_SERVER_THREADS_ACCEPTOR,
                Defaults.ENKI_SERVER_THREADS_ACCEPTOR,
                Integer.class);
        this.workerThreads   = getOptionalProperty(Keys.ENKI_SERVER_THREADS_WORKER,
                Defaults.ENKI_SERVER_THREADS_WORKER,
                Integer.class);
    }

    private static final class Keys {
        public static final String ENKI_ENV             = "enki.env";
        public static final String ENKI_ES_PATH_HOME    = "enki.es.path.home";
        public static final String ENKI_ES_CLUSTER_NAME = "enki.es.cluster.name";
        public static final String ENKI_KAFKA_BROKERS   = "enki.kafka.brokers";

        public static final String ENKI_ES_HTTP_ENABLED = "enki.es.http.enabled";
        public static final String ENKI_ES_HTTP_PORT    = "enki.es.http.port";

        public static final String ENKI_SERVER_BIND             = "enki.server.bind";
        public static final String ENKI_SERVER_PORT             = "enki.server.port";
        public static final String ENKI_SERVER_THREADS_ACCEPTOR = "enki.server.threads.acceptor";
        public static final String ENKI_SERVER_THREADS_WORKER   = "enki.server.threads.worker";
    }

    private static final class Defaults {
        public static final boolean ENKI_ES_HTTP_ENABLED = false;
        public static final int     ENKI_ES_HTTP_PORT    = 19216;

        public static final String  ENKI_SERVER_BIND             = "127.0.0.1";
        public static final int     ENKI_SERVER_PORT             = 6228;
        public static final int     ENKI_SERVER_THREADS_ACCEPTOR = 1;
        public static final int     ENKI_SERVER_THREADS_WORKER   = 10;
    }
}
