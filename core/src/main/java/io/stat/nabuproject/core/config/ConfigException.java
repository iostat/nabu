package io.stat.nabuproject.core.config;

/**
 * Created by io on 12/27/15. io is an asshole for
 * not giving writing documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class ConfigException extends Exception {
    ConfigException() {
        super();
    }
    ConfigException(String message) {
        super(message);
    }
    ConfigException(Throwable cause) {
        super(cause);
    }
    ConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
