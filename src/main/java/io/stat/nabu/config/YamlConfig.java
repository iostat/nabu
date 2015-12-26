package io.stat.nabu.config;

import io.stat.nabu.core.Component;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.ArrayList;

/**
 * A {@link Component} which provides configuration to Nabu.
 *
 * Honestly this whole thing is a bit of a huge hack.
 * But I want it provided as a service so you can do shit like runtime configuration reloading
 * without having a heavy dependency like Archaius or something like that.
 *
 * Created by io on 12/21/15. (929) 253-6977 $50/hr
 */
@Slf4j
public class YamlConfig extends ConfigurationProvider {
    private static final String NABU_YML_PATH = "nabu.yml";

    // actually used by lombok instead of the $lock it generates
    // reason I opted to manually specify what lock to use is because it may not always
    // be prudent to do a global lock against config, depending on what subsystems access what values
    @SuppressWarnings("unused")
    private final Object[] $_props_lock = new Object[] {};

    /**
     * Mapped to the nabu.env property
     */
    private @Getter(onMethod=@__(@Synchronized("$_props_lock"))) String env;

    /**
     * Mapped to the nabu.address property
     */
    private @Getter(onMethod=@__(@Synchronized("$_props_lock"))) String listenAddress;

    /**
     * Mapped to the nabu.port property
     */
    private @Getter(onMethod=@__(@Synchronized("$_props_lock"))) int listenPort;

    /**
     * Mapped to the nabu.threads.acceptor property
     */
    private @Getter(onMethod=@__(@Synchronized("$_props_lock"))) int acceptorThreads;
    /**
     * Mapped to the nabu.threads.worker property
     */
    private @Getter(onMethod=@__(@Synchronized("$_props_lock"))) int workerThreads;

    /**
     * Mapped to the nabu.es.path.home property
     */
    private @Getter(onMethod=@__(@Synchronized("$_props_lock"))) String eSHome;

    /**
     * Mapped to the nabu.es.http.enabled property
     */
    private @Getter(onMethod=@__(@Synchronized("$_props_lock"))) boolean eSHTTPEnabled;

    /**
     * Mapped to the nabu.es.http.port property
     */
    private @Getter(onMethod=@__(@Synchronized("$_props_lock"))) int eSHTTPPort;

    /**
     * Mapped to the nabu.kafka.brokers property
     */
    private @Getter(onMethod=@__(@Synchronized("$_props_lock"))) String[] kafkaBrokers;

    /**
     * The Apache Commons Configuration {@link PropertiesConfiguration} backing this YamlConfig
     */
    private PropertiesConfiguration backingProps;

    YamlConfig() {
        try {
            this.backingProps = new PropertiesConfiguration(NABU_YML_PATH);
        } catch(ConfigurationException ce) {
            throw new RuntimeException(ce);
        }

        this.env    = readRequiredProperty("nabu.env");
        this.eSHome = readRequiredProperty("nabu.es.path.home");
        this.kafkaBrokers = readRequiredStringArray("nabu.kafka.brokers");

        this.eSHTTPPort = readOptionalProperty("nabu.es.http.port", 19216);
        this.eSHTTPEnabled = readOptionalProperty("nabu.es.http.enable", "false").equals("true");

        this.listenAddress   = readOptionalProperty("nabu.address", "127.0.0.1");
        this.listenPort      = readOptionalProperty("nabu.port", 6228);
        this.acceptorThreads = readOptionalProperty("nabu.threads.acceptor", 1);
        this.workerThreads   = readOptionalProperty("nabu.threads.worker", 0);

    }

    @SuppressWarnings("unchecked")
    private <T> T readOptionalProperty(String key, T defaultValue) {
        if(backingProps.containsKey(key)) {
            T prop = (T) backingProps.getProperty(key);
            logger.trace("Optional property \"{}\" set to \"{}\"", key, prop);
            return prop;
        } else {
            logger.warn("Property \"{}\" not set, falling back to: \"{}\"", key, defaultValue);
            return defaultValue;
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T readRequiredProperty(String key) {
        if(backingProps.containsKey(key)) {
            T prop = (T) backingProps.getProperty(key);
            logger.trace("Required property \"{}\" set to \"{}\"", key, prop);
            return prop;
        } else {
            String message = "Required property " + key + " not set in bundled " + NABU_YML_PATH;
            logger.error(message);
            throw new RuntimeException(message);
        }
    }

    private String[] readRequiredStringArray(String key) {
        ArrayList<String> base = readRequiredProperty(key);
        String[] out = new String[base.size()];
        return base.toArray(out);
    }
}
