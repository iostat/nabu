package io.stat.nabuproject.core.net;

/**
 * An exception that is thrown when a network action cannot be completed due to the fact
 * that the node its directed at is leaving the cluster.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class NodeLeavingException extends Exception {
    public NodeLeavingException() {
        super();
    }

    public NodeLeavingException(String message) {
        super(message);
    }

    public NodeLeavingException(Throwable cause) {
        super(cause);
    }

    public NodeLeavingException(String message, Throwable cause) {
        super(message, cause);
    }
}
