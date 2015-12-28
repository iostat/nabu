package io.stat.nabuproject.core.config;

import com.google.inject.Inject;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESConfigProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * Configuration options that Nabu depends on.
 *
 * Note how there are concrete getters for basically every option that Nabu depends on.
 * They are the preferred form of accessing fields, as it ensures that there will always be
 * a valid configuration loaded.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public abstract class Config extends Component implements ESConfigProvider {
    private final ConfigurationProvider provider;

    protected <T> T getRequiredProperty(String key, Class<T> klass) throws ComponentException {
        T ret;

        try {
            ret = provider.getProperty(key, klass);
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

    protected <T> List<T> getRequiredSequence(String key, Class<T> klass) throws ComponentException {
        List<T> ret;

        try {
            ret = provider.getSequence(key, klass);
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

    protected Map<String, Object> getRequiredSubmap(String key) throws ComponentException {
        Map<String, Object> ret;

        try {
            ret = provider.getSubmap(key);
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

    protected <T> T getOptionalProperty(String key, T def, Class<T> klass) {
        T ret = def;
        try {
            ret = getRequiredProperty(key, klass);
        } catch(ComponentException e) {
            logger.debug("Lookup for optional property " + key + " failed", e);
        }

        if (ret == null) {
            logger.info("{} is not set and falling back to a default value of {}", key, def);
            return def;
        }

        return ret;
    }
}
