package com.socialrank.nabu.bootstrap;

import com.socialrank.nabu.config.NabuConfig;
import com.socialrank.nabu.config.NabuConfigException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by io on 12/21/15. (929) 253-6977 $50/hr
 */
@Slf4j
public final class NabuConfigLoader {
    private static final String NABU_YML_PATH = "nabu.yml";
    private static final List<String> requiredProperties = Collections.unmodifiableList(
            new ArrayList<String>() {{
                add("nabu.env");
            }}
    );

    public static NabuConfig parseBundledConfig() throws NabuConfigException {
        NabuConfigLoader instance;
        try {
            instance = new NabuConfigLoader();
        } catch(ConfigurationException ce) {
            throw new NabuConfigException(ce);
        }

        return instance.internalParseBundledConfig();
    }

    private final PropertiesConfiguration loadedProps;
    private final NabuConfig.NabuConfigBuilder builder;

    private NabuConfigLoader() throws ConfigurationException {
        this.loadedProps = new PropertiesConfiguration(NABU_YML_PATH);
        this.builder = NabuConfig.builder();
    }

    private NabuConfig internalParseBundledConfig() throws NabuConfigException {
        for(String requiredProperty : requiredProperties) {
            if (!loadedProps.containsKey(requiredProperty)) {
                throw new NabuConfigException("Required property " + requiredProperty + " not set!");
            }
        }

        builder.env(loadedProps.getString("nabu.env"));

        builder.listenAddress(getOptionalProperty("nabu.address", "127.0.0.1"));
        builder.listenPort(getOptionalProperty("nabu.port", 6228));

        builder.acceptorThreads(getOptionalProperty("nabu.threads.acceptor", 1));
        builder.workerThreads(getOptionalProperty("nabu.threads.worker", 0));

        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private <T> T getOptionalProperty(String key, T defaultValue) {
        if(loadedProps.containsKey(key)) {
            Object prop = loadedProps.getProperty(key);
            return (T) prop;
        } else {
            logger.info("Property \"" + key + "\" not set, falling back to: \"{}\"", defaultValue);
            return defaultValue;
        }
    }
}
