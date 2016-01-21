package org.elasticsearch.common;

import java.security.SecureRandom;

/**
 * Created by io on 1/21/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
class SecureRandomHolder {
    // class loading is atomic - this is a lazy & safe singleton to be used by this package
    public static final SecureRandom INSTANCE = new SecureRandom();
}
