package io.stat.nabuproject.enki.integration.balance;

import java.util.Set;

/**
 * Created by io on 1/8/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface AssignmentContext<T> {
    String getDescription();
    String collateAssignmentsReadably(Set<T> existing, Set<T> start, Set<T> stop);
}
