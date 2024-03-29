package io.stat.nabuproject.core;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A basic Component which starts Components in the order they are registered and stops
 * them the other way around. Needs to be expanded to make a dependency graph (either based on Guice dependencies
 * or some form of @DependsOn annotation for start() and stop()) so it can figure out how to do this by itself,
 * because having to manually specify the order that components have to be started is just frankly disgusting,
 * and can lead to really messed up bugs down the line.
 *
 * Due to the fact that it itself is a component, it may be possible to have components build these dep. graphs by themselves
 * by using ComponentStarter against the instances that are injected into them, and having some sort of system inside Component
 * to ensure that components which are transient or direct dependencies of others don't get start()'ed more than once.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public final class ComponentStarter extends Component {
    private Deque<Component> deque;
    private Set<Class<? extends Component>> registeredComponents;
    private AtomicBoolean isShuttingDown;
    private CountDownLatch startLatch;

    public ComponentStarter() {
        this.deque = new ArrayDeque<>();
        this.registeredComponents = new HashSet<>();
        this.isShuttingDown = new AtomicBoolean(false);
    }

    public void registerComponents(Component... cs) {
        Arrays.stream(cs).forEach(this::registerComponent);
    }

    public void registerComponent(Component c) {
        if(registeredComponents.contains(c.getClass())) {
            throw new IllegalArgumentException(c.getClass().getCanonicalName() + " is already registered in this ComponentStarter");
        } else {
            deque.addLast(c);
        }
    }

    @Override
    public void start() throws ComponentException {
        // don't use deque.forEach unless you want terrible exception bubbling.
        this.startLatch = new CountDownLatch(deque.size());
        for (Component c : deque) {
            try {
                c.setStarter(this);

                if(!this.isShuttingDown.get()) {
                    c._dispatchStart();
                } else {
                    logger.error("Cannot start {} as this starter is shutting down", c);
                }
            } catch(Exception e) {
                logger.error("An exception bubbled up when trying to start a component", e);
            } finally {
                startLatch.countDown();
            }
        }
    }

    @Override
    public void shutdown() throws ComponentException {
        this.isShuttingDown.set(true);
        try {
            startLatch.await(5, TimeUnit.SECONDS);
        } catch(InterruptedException e) {
            logger.error("Failed to await startup to finish before shutting down.", e);
        }
        Iterator<Component> rit = deque.descendingIterator();
        while(rit.hasNext()) {
            rit.next()._dispatchShutdown();
        }

        if(getStarter() != null) {
            getStarter().shutdown();
        }
    }
}
