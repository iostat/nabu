package io.stat.nabuproject.core.config;

/**
 * Thrown when there is a error in performing a substitution
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class SubstitutionException extends ConfigException {
    public SubstitutionException() {
        super();
    }

    public SubstitutionException(String message) {
        super(message);
    }

    public SubstitutionException(Throwable cause) {
        super(cause);
    }

    public SubstitutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
