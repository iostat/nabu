package io.stat.nabuproject.core.net;

/**
 * An exception that implies that a connection was lost unexpectedly to either Nabu or Enki.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ConnectionLostException extends Exception {
    public ConnectionLostException() {
        super();
    }
    public ConnectionLostException(String message) {
        super(message);
    }
    public ConnectionLostException(Throwable cause) {
        super(cause);
    }
    public ConnectionLostException(String message, Throwable cause) {
        super(message, cause);
    }
}
