package io.stat.nabuproject.core.config;

import java.util.List;
import java.util.Map;

/**
 * Specification for something which can provide configuration
 * options that the Nabu/Enki applications depend on. An implementation must simply
 * provide {@link ConfigStore#getProperty(String, Class)},
 * {@link ConfigStore#getSequence(String, Class)}, and {@link ConfigStore#getSubmap(String)}.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ConfigStore {
    /**
     * Get a scalar property from the configuration store. The key
     * will be a flattened representation (e.g. for <tt>nabuproject.env</tt>,
     * if a structured configuration format is used, it would imply the
     * subkey <tt>env</tt> inside the root key <tt>nabuproject</tt>).
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
    <T> T getProperty(String key, Class<T> klass) throws ConfigException;

    /**
     * Get a sequence property from the configuration store. Just like with
     * {@link ConfigStore#getProperty(String, Class)}, the key will given in a flattened
     * representation. If there is a scalar value assigned to the the key, you may treat it as
     * a single-element sequence.
     *
     * @param key the key of the property which contains the sequence.
     * @param klass the class of T
     * @param <T> the expected type of the value. it is safe to assume that T will always be a primitive or {@link String}
     * @return a {@link List} of the values in the sequence.
     * @throws ConfigException if the key doesn't exist,
     */
    <T> List<T> getSequence(String key, Class<T> klass) throws ConfigException;

    /**
     * Get a submap from the configuration store. Much like with
     * {@link ConfigStore#getProperty(String, Class)}
     * and {@link ConfigStore#getSequence(String, Class)} the key passed in
     * will be given as a flattened representation. The expected output is a
     * Map&lt;String, Object&gt;, which means that if your backing store is
     * not in a structured format, you will be expect to emulate that.
     *
     * @param key the key of the requested root property.
     * @return a submap of entries which are children of <tt>key</tt>
     * @throws ConfigException if the key doesn't exist, or is of a scalar or sequence type
     */
    Map<String, Object> getSubmap(String key) throws ConfigException;
}
