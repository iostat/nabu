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
 * Represents a system-wide configuration, generally loaded from a file.
 * By default, it is required to implement {@link ESConfigProvider}
 * since both Nabu and Enki depend on ES to get bootstrapped.
 *
 * Nabu and Enki extend this class in their own modules (see NabuConfig and EnkiConfig in their respective javadoc trees)
 *
 * Generally, a module has its own interface for ConfigProvider which gets dependency injected. The nabu-/enki-specific dependency
 * bindings are specified at the start of each application in their Guice injector.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public abstract class AbstractConfig extends Component implements ESConfigProvider {
    private final ConfigStore provider;

    /**
     * Can be used by children to read a property, and ensure it exists and is valid.
     * A property can be any Java primitive type, or any type that implements {@link MappedConfigObject}.
     * To get a property which is supposed to be a List of the above, see {@link AbstractConfig#getRequiredSequence(String, Class)}
     *
     * @param key The key under which the property is found
     * @param klass the Class of <tt>&lt;T&gt;</tt> since reified generics are too un-enterprise for Java
     * @param <T> the type that <tt>klass</tt> represents.
     * @return the value that the ConfigStore returned from its backing store.
     * @throws ComponentException if there was a problem reading the requested property
     */
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

    /**
     * Similar to {@link AbstractConfig#getRequiredProperty(String, Class)}, except it is for getting
     * Lists of properties.
     *
     * @param key The key under which the property is found
     * @param klass the Class of <tt>&lt;T&gt;</tt> since reified generics are too un-enterprise for Java
     * @param <T> the type that <tt>klass</tt> represents.
     * @return the value that the ConfigStore returned from its backing store.
     * @throws ComponentException if there was a problem reading the requested property
     */
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

    /**
     * Returns a raw Map of all subproperties of a key. This is used when reifying non-primitive subtypes, but is also
     * left in here for the convenience of implementors.
     *
     * @param key the key under which to get a submap
     * @return a <tt>Map&lt;String, Object&gt;</tt> of the submap
     * @throws ComponentException if there was a problem reading the requested property
     */
    protected final Map<String, Object> getRequiredSubmap(String key) throws ComponentException {
        Map<String, Object> ret;

        try {
            ret = provider.getSubmap(key);
        } catch (ConfigException ce) {
            String message = "Could not get the value of required submap " + key;
            throw new ComponentException(true, message, ce);
        }

        if(ret == null) {
            String message = "Got a null back from the provider for getSubmap. This is an incorrect implementation.";
            logger.error(message);
            throw new ComponentException(true, message);
        }

        return ret;
    }

    /**
     * Similar to {@link AbstractConfig#getRequiredProperty(String, Class)}, except that if a {@link MissingPropertyException}
     * is thrown, a warning is printed and the value of <tt>fallback</tt> is returned. Any other exceptions get bubbled up.
     *
     * @param key The key under which the property is found
     * @param fallback the value to default to if the requested property is not set.
     * @param klass the Class of <tt>&lt;T&gt;</tt> since reified generics are too un-enterprise for Java
     * @param <T> the type that <tt>klass</tt> represents.
     * @throws ComponentException if the cause of the exception was not a {@link MissingPropertyException}
     * @return the value which was set if it was set, or <tt>fallback</tt> if it wasn't
     */
    protected final <T> T getOptionalProperty(String key, T fallback, Class<T> klass) throws ComponentException {
        try {
            return getRequiredProperty(key, klass);
        } catch(ComponentException e) {
            if(e.getCause() instanceof MissingPropertyException) {
                logger.info("{} is not set and falling back to a default value of {}", key, fallback);
                return fallback;
            } else {
                // wrap in another ComponentException so that it's clear that this got thrown in getOptionalProperty.
                throw new ComponentException(e);
            }
        }
    }

    /**
     * {@link AbstractConfig#getOptionalSequence(String, List, Class)} is to {@link AbstractConfig#getRequiredSequence(String, Class)}
     * as {@link AbstractConfig#getOptionalProperty(String, Object, Class)} is to {@link AbstractConfig#getRequiredProperty(String, Class)}
     *
     * @param key The key under which the property is found
     * @param fallback a List of <tt>&lt;T&gt;s</tt> to default to if a property is missing.
     * @param klass the Class of <tt>&lt;T&gt;</tt> since reified generics are too un-enterprise for Java
     * @param <T> the type that <tt>klass</tt> represents.* @return
     * @throws ComponentException
     * @return the list of values which was set if it was set, or <tt>fallback</tt> if it wasn't
     */
    protected final <T> List<T> getOptionalSequence(String key, List<T> fallback, Class<T> klass) throws ComponentException {
        try {
            return getRequiredSequence(key, klass);
        } catch(ComponentException e) {
            if(e.getCause() instanceof MissingPropertyException) {
                logger.info("{} is not set and falling back to a default value of {}", key, fallback);
                return fallback;
            } else {
                // wrap in another ComponentException so that it's clear that this got thrown in getOptionalSequence
                throw new ComponentException(e);
            }
        }
    }

    /**
     * Just like {@link AbstractConfig#getRequiredSubmap(String)}, just 100% more optional.
     * @param key the key to find the map under
     * @param fallback the map to fall back to
     * @return the entire submap underneath <tt>key</tt> if it was set, or <tt>fallback</tt> if it wasn't
     */
    protected final Map<String, Object> getOptionalSubmap(String key, Map<String, Object> fallback) {
        try {
            return getRequiredSubmap(key);
        } catch(ComponentException e) {
            if(e.getCause() instanceof MissingPropertyException) {
                logger.info("{} is not set and falling back to a default value of {}", key, fallback);
                return fallback;
            } else {
                throw new ComponentException(e);
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
