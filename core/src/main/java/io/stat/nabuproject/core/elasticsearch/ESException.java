package io.stat.nabuproject.core.elasticsearch;

/**
 * An ElasticSearch related exception.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ESException extends RuntimeException {
    public ESException() {
        super();
    }

    public ESException(String message) {
        super(message);
    }

    public ESException(Throwable cause) {
        super(cause);
    }

    public ESException(String message, Throwable cause) {
        super(message, cause);
    }
}
