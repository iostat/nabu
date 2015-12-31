package io.stat.nabuproject.core;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

    public ComponentStarter() {
        this.deque = new ArrayDeque<>();
        this.registeredComponents = new HashSet<>();
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
        deque.forEach(Component::_dispatchStart);
    }

    @Override
    public void shutdown() throws ComponentException {
        for(Iterator<Component> rit = deque.descendingIterator(); rit.hasNext();) {
            rit.next()._dispatchShutdown();
        }
    }
}
