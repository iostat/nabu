package io.stat.nabuproject.core;

/**
 * Describes a class which can route method calls to an instance of
 * T at runtime. Provides support for changing the instance of T
 * at runtime, but otherwise acts effectively as a tag interface,
 * as the component itself should act as a T and do the actual routing.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface DynamicComponent<T>  {
    void replaceInstance(T newInstance);
    T getCurrentInstance();
}
