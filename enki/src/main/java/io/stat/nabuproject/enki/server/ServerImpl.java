package io.stat.nabuproject.enki.server;

import com.google.inject.Inject;
import io.stat.nabuproject.enki.EnkiConfig;

/**
 * Created by io on 12/28/15. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
class ServerImpl extends EnkiServer {
    private final EnkiConfig config;

    @Inject
    ServerImpl(EnkiConfig config) {
        this.config = config;
    }
}
