package io.stat.nabuproject.core.enkiprotocol;

/**
 * Created by io on 1/2/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface EnkiConnection {
    void leave();
    void onDisconnected();

}
