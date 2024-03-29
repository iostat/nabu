package io.stat.nabuproject.core;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a component of the Nabu system. Every child class of this
 * can be instantiated as a singleton for the Nabu application's lifecycle.
 *
 * This thus provides rudimentary lifecycle management for each "subsystem" of
 * Nabu.
 *
 * Note that all the lifecycle management methods are called synchronously. This means that
 * if your component blocks anywhere, you may very well hang the whole application.
 */
@Slf4j @EqualsAndHashCode
public abstract class Component implements IComponent {
    /**
     * (internal) validates that this component is in a valid state to start and dispatches
     * a call to {@link Component#start()}
     * @throws ComponentException if any bubbled up from {@link Component#start()}
     */
    final void _dispatchStart() throws ComponentException {
        if(wasStopped()) {
            logger.warn("Trying to start() a {} that's already been stopped before.", this.getClass().getCanonicalName());
            return;
        }

        if(wasStarted()) {
            logger.warn("Start()'ing a {} that's already been start()'d!", this.getClass().getCanonicalName());
        }

        start();
        _started = true;
    }

    /**
     * (internal) validates that this component is in a valid state to shutdown and dispatches
     * a call to {@link Component#shutdown()}
     * @throws ComponentException if any bubbled up from {@link Component#shutdown()}
     */
    final void _dispatchShutdown() throws ComponentException {
        if(wasStopped()) { logger.warn("Trying to shutdown an already stopped {}", this.getClass().getCanonicalName()); return; }
        if(!wasStarted()) {
            logger.warn("Trying to shutdown a non-started {}", this.getClass().getCanonicalName());
            return;
        }

        shutdown();
        _stopped = true;
    }

    /**
     * @return Whether or not this {@link Component} was started.
     */
    public final boolean wasStarted() {
        return _started;
    }

    /**
     * @return Whether or not this {@link Component} was stopped.
     */
    public final boolean wasStopped() {
        return _stopped;
    }

    private boolean _started = false;
    private boolean _stopped = false;

    /**
     * Lombok helper for methods which can't be delegated, when trying to delegate two levels of Component.
     */
    public interface Undelegatable {
        boolean wasStarted();
        boolean wasStopped();
        void _dispatchStart();
        void _dispatchShutdown();
    }

    private @Getter @Setter IComponent starter;
}
