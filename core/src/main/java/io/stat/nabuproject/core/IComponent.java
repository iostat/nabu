package io.stat.nabuproject.core;

/**
 * The interface counterpart to {@link Component},
 * because frankly the idea of writing <tt>AbstractComponent</tt> everywhere
 * just horrifies me.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface IComponent {
    /**
     * Called *synchronously* when the component is started.
     *
     * @throws ComponentException if something went wrong while starting the Component
     */
    default void start() throws ComponentException { }

    /**
     * Called *synchronously* when Nabu shuts down, clean up and prepare for shutdown.
     *
     * @throws ComponentException if something went wrong while shutting down the Component
     */
    default void shutdown() throws ComponentException { }
}
