package io.stat.nabu.config;

/**
 * Thrown when a mandatory property is not set.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class MissingPropertyException extends ConfigException {
    MissingPropertyException() {
        super();
    }

    MissingPropertyException(String message) {
        super(message);
    }

    MissingPropertyException(Throwable cause) {
        super(cause);
    }

    MissingPropertyException(String message, Throwable cause) {
        super(message, cause);
    }
}
