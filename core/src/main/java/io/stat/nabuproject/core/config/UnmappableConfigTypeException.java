package io.stat.nabuproject.core.config;

/**
 * Thrown when there is a problem reifying a config submap to a
 * class. Specifically, it means that the class does not implement
 * #{link MappedConfigObject}
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
