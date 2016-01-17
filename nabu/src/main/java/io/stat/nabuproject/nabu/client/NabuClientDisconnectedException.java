package io.stat.nabuproject.nabu.client;

/**
 * Thrown when an operation that requires an active connection
 * fails because one is not available.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class NabuClientDisconnectedException extends NabuClientException {
    public NabuClientDisconnectedException() {
        super();
    }

    public NabuClientDisconnectedException(String message) {
        super(message);
    }

    public NabuClientDisconnectedException(Throwable cause) {
        super(cause);
    }

    public NabuClientDisconnectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
