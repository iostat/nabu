package io.stat.nabuproject.core;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A basic thing which starts Components in the order they are added and stops
 * them the other way around. Needs to be expanded to make a dependency graph (based
 * on Guice dependencies) so it can figure out how to do this by itself.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public final class ComponentStarter {
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

    public void start() throws ComponentException {
        deque.forEach(Component::_dispatchStart);
    }

    public void shutdown() throws ComponentException {
        for(Iterator<Component> rit = deque.descendingIterator(); rit.hasNext();) {
            rit.next()._dispatchShutdown();
        }
    }
}
