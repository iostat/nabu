package io.stat.nabuproject.core.util;

/**
 * Thrown when shit has REALLY hit the fan.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class CatastrophicException extends Exception {
    public CatastrophicException(String message) {
        super(message);
    }

    public CatastrophicException(String message, Throwable cause) {
        super(message, cause);
    }
}
