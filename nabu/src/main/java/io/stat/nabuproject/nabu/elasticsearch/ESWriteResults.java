package io.stat.nabuproject.nabu.elasticsearch;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Simple Value class for storing
 * whether or not the write was successful,
 * and how long the ES write took.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor @EqualsAndHashCode @ToString
public final class ESWriteResults {
    final boolean wasSuccess;
    final long timeTook;
    final @Getter String message;
    public static final String DEFAULT_WRITER_MESSAGE = "<no message specified>";

    public ESWriteResults(boolean wasSuccess, long timeTook) {
        this(wasSuccess, timeTook, DEFAULT_WRITER_MESSAGE);
    }


    /**
     * How long the operation took.
     * @return How long executing the operation took, in nanoseconds
     */
    public long getTime() {
        return timeTook;
    }

    /**
     * @return whether or not the operation executed successfully.
     */
    public boolean success() {
        return wasSuccess;
    }
}
