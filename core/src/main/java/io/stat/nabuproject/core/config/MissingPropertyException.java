package io.stat.nabuproject.core.config;

/**
 * Thrown when a mandatory property is not set.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class MissingPropertyException extends ConfigException {
    public MissingPropertyException() {
        super();
    }

    public MissingPropertyException(String message) {
        super(message);
    }

    public MissingPropertyException(Throwable cause) {
        super(cause);
    }

    public MissingPropertyException(String message, Throwable cause) {
        super(message, cause);
    }
}
