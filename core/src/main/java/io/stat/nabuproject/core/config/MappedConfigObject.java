package io.stat.nabuproject.core.config;

/**
 * A class which can be rebuilt from a Map&lt;String, Object&gt;
 *
 * @param <T> should always be the same as the implementor.
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface MappedConfigObject<T extends MappedConfigObject> {
    /**
     * Return a {@link ConfigMapper} which can an instance of this object.
     *
     * IMPORTANT: for the sake of simplicity, an instance of this MappedConfigObject is created without any constructors
     * being called. getMapper() should UNDER NO CIRCUMSTANCES depend on ANY state of the object.
     *
     * @return a ConfigMapper that can create an instance of this type of MappedConfigObject.
     */
    ConfigMapper<T> getMapper();
}
