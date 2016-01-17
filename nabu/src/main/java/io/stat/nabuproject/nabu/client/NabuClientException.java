package io.stat.nabuproject.nabu.client;

/**
 * Superclass for all exception that can be thrown by the NabuClient
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class NabuClientException extends Exception {
    public NabuClientException() {
        super();
    }

    public NabuClientException(String message) {
        super(message);
    }

    public NabuClientException(Throwable cause) {
        super(cause);
    }

    public NabuClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
