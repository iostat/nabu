package io.stat.nabuproject.core.config;

import com.google.inject.Inject;
import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.elasticsearch.ESConfigProvider;
import io.stat.nabuproject.core.util.JVMHackery;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
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

    protected final <T> T getRequiredProperty(String key, Class<T> klass) throws ComponentException {
        ensureClassIsConfigurable(klass);
        if(MappedConfigObject.class.isAssignableFrom(klass)) {
            try {
                return reifyMap(getRequiredSubmap(key), klass);
            } catch(ConfigException ce) { throw new ComponentException(true, ce); }
        }

        T ret;

        try {
            ret = provider.getProperty(key, klass);
        } catch (ConfigException ce) {
            String message = "Could not get the value of required property " + key;
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

    protected final <T> List<T> getRequiredSequence(String key, Class<T> klass) throws ComponentException {
        ensureClassIsConfigurable(klass);
        if(MappedConfigObject.class.isAssignableFrom(klass)) {
            try {
                List<Map> intermediary = provider.getSequence(key, Map.class);
                List<T> ret = new ArrayList<>(intermediary.size());

                for(Map m : intermediary) {
                    ret.add(reifyMap(m, klass));
                }

                return ret;
            } catch(ConfigException ce) { throw new ComponentException(true, ce); }

        }

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

    // todo: hack hack hack hack hack hack hack hack hack hack hack hack hack hack
    private <T> T reifyMap(Map theMap, Class<T> klass) throws ConfigException {
        MappedConfigObject mco = (MappedConfigObject) JVMHackery.createUnsafeInstance(klass);
        return (T) mco.getMapper().buildFromMap(theMap);
    }

    protected final Map<String, Object> getRequiredSubmap(String key) throws ComponentException {
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

    protected final <T> T getOptionalProperty(String key, T def, Class<T> klass) {
        try {
            return getRequiredProperty(key, klass);
        } catch(ComponentException e) {
            if(e.getCause() instanceof MissingPropertyException) {
                logger.info("{} is not set and falling back to a default value of {}", key, def);
                return def;
            } else {
                throw e;
            }
        }
    }

    protected final <T> List<T> getOptionalSequence(String key, List<T> def, Class<T> klass) {
        try {
            return getRequiredSequence(key, klass);
        } catch(ComponentException e) {
            if(e.getCause() instanceof MissingPropertyException) {
                logger.info("{} is not set and falling back to a default value of {}", key, def);
                return def;
            } else {
                throw e;
            }
        }
    }

    private void ensureClassIsConfigurable(Class klass) throws ComponentException {
        if (Number.class.isAssignableFrom(klass)    ||
            Character.class.isAssignableFrom(klass) ||
            String.class.isAssignableFrom(klass)    ||
            Boolean.class.isAssignableFrom(klass)   ||
            MappedConfigObject.class.isAssignableFrom(klass)) {
            return;
        }

        throw new ComponentException(true, new UnmappableConfigTypeException(klass.getCanonicalName() + " does not implement ConfigMapper!"));
    }
}
