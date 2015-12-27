package io.stat.nabu.config;

/**
 * An exception may be thrown by the configuration system when a required
 * property is not set.
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
