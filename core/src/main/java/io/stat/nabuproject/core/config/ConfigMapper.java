package io.stat.nabuproject.core.config;

import java.util.Map;

/**
 * Represents a class which create an instance of <tt>T</tt> from a
 * <tt>Map&lt;String, Object&gt;</tt>.
 * @param <T> the type of Object this mapper can construct.
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface ConfigMapper<T extends MappedConfigObject> {
    /**
     * Convert a Map into a T
     * @param map the source map
     * @return a T constructed with parameters in the Map
     */
    T buildFromMap(Map<String, Object> map) throws ConfigException;
}
