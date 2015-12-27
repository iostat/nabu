package io.stat.nabu.config;

import io.stat.nabu.core.Component;
import io.stat.nabu.core.ComponentException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * Specification for something which can provide configuration
 * options that the Nabu application depends on. An implementation must simply
 * provide {@link ConfigurationProvider#getProperty(String, Class)},
 * {@link ConfigurationProvider#getSequence(String, Class)}, and {@link ConfigurationProvider#getSubmap(String)}
 * and ConfigurationProvider handles the rest.
 *
 * Note that there are concrete getters for basically every option that Nabu depends on.
 * They are the preferred form of accessing fields, as it ensures that there will always be
 * a valid configuration loaded.
 *
 * An inheritor MUST call {@link ConfigurationProvider#setupComplete()} in its constructor after it's loaded all it needs, otherwise,
 * nobody using the config will be able to get any data (ConfigurationProvider itself validates and caches all necessary values)
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public abstract class ConfigurationProvider extends Component {
    /**
     * Get a scalar property from the configuration store. The key
     * will be a flattened representation (e.g. for <tt>nabu.env</tt>,
     * if a structured configuration format is used, it would imply the
     * subkey <tt>env</tt> inside the root key <tt>nabu</tt>).
     *
     * You may throw a {@link ConfigException} if the key does not exist or cannot be
     * represented as the requested type.
     *
     * @param key the key of the property
     * @param klass the class of T
     * @param <T> the expected type of the value. it is safe to assume that T will always be a primitive or {@link String}
     * @return the value of the requested key
     * @throws ConfigException if the key doesn't exist, or cannot be represented as the requested type
     */
    protected abstract <T> T getProperty(String key, Class<T> klass) throws ConfigException;

    /**
     * Get a sequence property from the configuration store. Just like with
     * {@link ConfigurationProvider#getProperty(String, Class)}, the key will given in a flattened
     * representation. If there is a scalar value assigned to the the key, you may treat it as
     * a single-element sequence.
     *
     * @param key the key of the property which contains the sequence.
     * @param klass the class of T
     * @param <T> the expected type of the value. it is safe to assume that T will always be a primitive or {@link String}
     * @return a {@link List} of the values in the sequence.
     * @throws ConfigException if the key doesn't exist,
     */
    protected abstract <T> List<T> getSequence(String key, Class<T> klass) throws ConfigException;

    /**
     * Get a submap from the configuration store. Much like with
     * {@link ConfigurationProvider#getProperty(String, Class)}
     * and {@link ConfigurationProvider#getSequence(String, Class)} the key passed in
     * will be given as a flattened representation. The expected output is a
     * Map&lt;String, Object&gt;, which means that if your backing store is
     * not in a structured format, you will be expect to emulate that.
     *
     * @param key the key of the requested root property.
     * @return a submap of entries which are children of <tt>key</tt>
     * @throws ConfigException if the key doesn't exist, or is of a scalar or sequence type
     */
    protected abstract Map<String, Object> getSubmap(String key) throws ConfigException;

    /**
     * Mapped to the nabu.env property
     */
    private @Getter String env;

    /**
     * Mapped to the nabu.server.bind property
     */
    private @Getter String listenAddress;

    /**
     * Mapped to the nabu.server.port property
     */
    private @Getter int listenPort;

    /**
     * Mapped to the nabu.server.threads.acceptor property
     */
    private @Getter int acceptorThreads;
    /**
     * Mapped to the nabu.server.threads.worker property
     */
    private @Getter int workerThreads;

    /**
     * Mapped to the nabu.es.path.home property
     */
    private @Getter String eSHome;

    /**
     * Mapped to the nabu.es.http.enabled property
     */
    private @Getter boolean eSHTTPEnabled;

    /**
     * Mapped to the nabu.es.http.port property
     */
    private @Getter int eSHTTPPort;

    /**
     * Mapped to the nabu.kafka.brokers property
     */
    private @Getter List<String> kafkaBrokers;

    /**
     * Mapped to the nabu.es.cluster.name property
     */
    private @Getter String eSClusterName;


    protected final void setupComplete() throws ComponentException {
        this.env    = getRequiredProperty(KeyNames.NABU_ENV, String.class);
        this.eSHome = getRequiredProperty(KeyNames.NABU_ES_PATH_HOME, String.class);
        this.eSClusterName = getRequiredProperty(KeyNames.NABU_ES_CLUSTER_NAME, String.class);
        this.kafkaBrokers  = getRequiredSequence(KeyNames.NABU_KAFKA_BROKERS, String.class);

        this.eSHTTPEnabled = getOptionalProperty(KeyNames.NABU_ES_HTTP_ENABLED, OptionalDefaults.NABU_ES_HTTP_ENABLED, Boolean.class);
        this.eSHTTPPort    = getOptionalProperty(KeyNames.NABU_ES_HTTP_PORT, OptionalDefaults.NABU_ES_HTTP_PORT, Integer.class);

        this.listenAddress = getOptionalProperty(KeyNames.NABU_SERVER_BIND, OptionalDefaults.NABU_SERVER_BIND, String.class);
        this.listenPort    = getOptionalProperty(KeyNames.NABU_SERVER_PORT, OptionalDefaults.NABU_SERVER_PORT, Integer.class);

        this.acceptorThreads = getOptionalProperty(KeyNames.NABU_SERVER_THREADS_ACCEPTOR,
                OptionalDefaults.NABU_SERVER_THREADS_ACCEPTOR,
                Integer.class);
        this.workerThreads   = getOptionalProperty(KeyNames.NABU_SERVER_THREADS_WORKER,
                OptionalDefaults.NABU_SERVER_THREADS_WORKER,
                Integer.class);
    }

    private <T> T getRequiredProperty(String key, Class<T> klass) throws ComponentException {
        T ret;

        try {
            ret = getProperty(key, klass);
        } catch (ConfigException ce) {
            String message = "Could not get the value of required property " + key;
            logger.error(message, ce);
            throw new ComponentException(true, message, ce);
        }

        if(ret == null) {
            String message = "Got a null back from the provider for getProperty. This is an incorrect implementation.";
            logger.error(message);
            throw new ComponentException(true, message);
        }

        if(ret.getClass().isAssignableFrom(String.class) && ret.toString().trim().isEmpty()) {
            String message = "Found key " + key + " and it is expected to be a String, but it is empty.";
            logger.error(message);
            throw new ComponentException(true, message);
        }

        return ret;
    }
    private <T> List<T> getRequiredSequence(String key, Class<T> klass) throws ComponentException {
        List<T> ret;

        try {
            ret = getSequence(key, klass);
        } catch (ConfigException ce) {
            String message = "Could not get the value of required sequence " + key;
            logger.error(message, ce);
            throw new ComponentException(true, message, ce);
        }

        if(ret == null) {
            String message = "Got a null back from the provider for getSequence. This is an incorrect implementation.";
            logger.error(message);
            throw new ComponentException(true, message);
        }

        return ret;
    }
    private Map<String, Object> getRequiredSubmap(String key) throws ComponentException {
        Map<String, Object> ret;

        try {
            ret = getSubmap(key);
        } catch (ConfigException ce) {
            String message = "Could not get the value of required sequence " + key;
            logger.error(message, ce);
            throw new ComponentException(true, message, ce);
        }

        if(ret == null) {
            String message = "Got a null back from the provider for getSubmap. This is an incorrect implementation.";
            logger.error(message);
            throw new ComponentException(true, message);
        }

        return ret;
    }

    private <T> T getOptionalProperty(String key, T def, Class<T> klass) {
        T ret = def;
        try {
            ret = getRequiredProperty(key, klass);
        } catch(ComponentException e) {
            logger.trace("Lookup for optional property " + key + " failed", e);
        }

        if (ret == null) {
            logger.warn("{} is not set and falling back to a default value of {}", key, def);
            return def;
        }

        return ret;
    }

    public static final class KeyNames {
        public static final String NABU_ENV             = "nabu.env";
        public static final String NABU_ES_PATH_HOME    = "nabu.es.path.home";
        public static final String NABU_ES_CLUSTER_NAME = "nabu.es.cluster.name";
        public static final String NABU_KAFKA_BROKERS   = "nabu.kafka.brokers";

        public static final String NABU_ES_HTTP_ENABLED = "nabu.es.http.enabled";
        public static final String NABU_ES_HTTP_PORT    = "nabu.es.http.port";

        public static final String NABU_SERVER_BIND = "nabu.server.bind";
        public static final String NABU_SERVER_PORT = "nabu.server.port";
        public static final String NABU_SERVER_THREADS_ACCEPTOR = "nabu.server.threads.acceptor";
        public static final String NABU_SERVER_THREADS_WORKER   = "nabu.server.threads.worker";
    }

    public static final class OptionalDefaults {
        public static final boolean NABU_ES_HTTP_ENABLED = false;
        public static final int     NABU_ES_HTTP_PORT    = 19216;

        public static final String NABU_SERVER_BIND = "127.0.0.1";
        public static final int    NABU_SERVER_PORT = 6228;
        public static final int    NABU_SERVER_THREADS_ACCEPTOR = 1;
        public static final int    NABU_SERVER_THREADS_WORKER   = 10;
    }
}
