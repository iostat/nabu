package io.stat.nabuproject.core.config;

/**
 * Thrown when the configuration contains a value that
 * does not make sense.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class InvalidValueException extends ConfigException {
    public InvalidValueException() {
        super();
    }

    public InvalidValueException(String message) {
        super(message);
    }

    public InvalidValueException(Throwable cause) {
        super(cause);
    }

    public InvalidValueException(String message, Throwable cause) {
        super(message, cause);
    }
}
