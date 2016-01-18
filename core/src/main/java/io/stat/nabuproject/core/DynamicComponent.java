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
    /**
     * Replaces the instance that this DynamicComponent provides with
     * newInstance
     * @param newInstance the instance to replace the served instance of
     *                    the dynamic component with.
     */
    void replaceInstance(T newInstance);

    /**
     * Resets the instance of the provided component to the default instance
     * this DynamicComponent was set to provide.
     */
    void setToDefaultInstance();

    /**
     * Gets the current instance that this DynamicComponent routes requests to
     * @return the instance that requests are being routed to
     */
    T getCurrentInstance();

    /**
     * Gets the class of the default instance that this DynamicProvider would route
     * to if {@link DynamicComponent#replaceInstance(Object)} was never called
     * or {@link DynamicComponent#setToDefaultInstance()} is called.
     * @return the class of the default instance this DynamicComponent will provide
     */
    Class<? extends T> getClassOfDefaultInstance();

    /**
     * Gets the instance of T that this DynamicComponent routes to when first created
     */
    T getDefaultInstance();
}
