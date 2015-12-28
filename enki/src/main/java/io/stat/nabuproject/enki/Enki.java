package io.stat.nabuproject.enki;

import io.stat.nabuproject.core.ComponentException;

/**
 * Created by io on 12/28/15. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface Enki {
    void start() throws ComponentException;
    void shutdown() throws ComponentException;
}
