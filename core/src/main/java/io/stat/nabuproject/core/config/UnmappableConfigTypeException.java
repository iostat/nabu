package io.stat.nabuproject.core.config;

/**
 * Created by io on 12/29/15. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class UnmappableConfigTypeException extends ConfigException {
    UnmappableConfigTypeException() {
        super();
    }
    UnmappableConfigTypeException(String message) {
        super(message);
    }
    UnmappableConfigTypeException(Throwable cause) {
        super(cause);
    }
    UnmappableConfigTypeException(String message, Throwable cause) {
        super(message, cause);
    }
}
