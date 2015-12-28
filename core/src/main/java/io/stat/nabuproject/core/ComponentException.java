package io.stat.nabuproject.core;

import lombok.Getter;

/**
 * Represents an exception that can be thrown at any point during a
 * {@link Component}'s lifecycle management operation.
 */
public class ComponentException extends RuntimeException {
    /**
     * Whether or not this Exception is "fatal" and warrants a full shutdown of Nabu.
     */
    private @Getter boolean fatal = false;

    public ComponentException() {
        super();
    }
    public ComponentException(String message) {
        super(message);
    }
    public ComponentException(Throwable cause) {
        super(cause);
    }
    public ComponentException(String message, Throwable cause) {
        super(message, cause);
    }
    public ComponentException(boolean fatal) {
        super();
        this.fatal = fatal;
    }
    public ComponentException(boolean fatal, String message) {
        super(message);
        this.fatal = fatal;
    }
    public ComponentException(boolean fatal, Throwable cause) {
        super(cause);
        this.fatal = fatal;
    }
    public ComponentException(boolean fatal, String message, Throwable cause) {
        super(message, cause);
        this.fatal = fatal;
    }
}
