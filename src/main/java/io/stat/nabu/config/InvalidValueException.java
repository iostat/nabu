package io.stat.nabu.config;

/**
 * Created by io on 12/27/15. io is an asshole for
 * not giving writing documentation for his code.
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
