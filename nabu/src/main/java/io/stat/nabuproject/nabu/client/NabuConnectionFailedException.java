package io.stat.nabuproject.nabu.client;

/**
 * Thrown when {@link NabuClient} has exhausted its list of servers to attempt to connect to.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class NabuConnectionFailedException extends Exception {
    public NabuConnectionFailedException() {
        super();
    }

    public NabuConnectionFailedException(String message) {
        super(message);
    }

    public NabuConnectionFailedException(Throwable cause) {
        super(cause);
    }

    public NabuConnectionFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
