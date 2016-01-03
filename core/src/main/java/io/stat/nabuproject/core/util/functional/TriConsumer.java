package io.stat.nabuproject.core.util.functional;

import java.util.Objects;

/**
 * Basically like BiConsumer, except it takes three arguments!
 * @param <T> type of first arg
 * @param <U> type of second arg
 * @param <V> type of third arg
 */
@FunctionalInterface
public interface TriConsumer<T, U, V> {
    /**
     * Do something with
     * @param t arg the 1st
     * @param u arg the 2nd
     * @param v arg the 3rd
     */
    void accept(T t, U u, V v);

    /**
     * Allows you chain together two TriConsumers to run one after the other.
     * @param after the consumer to run after this one.
     * @return a TriConsumer that runs this consumer, followed by <tt>after</tt>
     */
    default TriConsumer<T, U, V> andThen(TriConsumer<? super T, ? super U, ? super V> after) {
        Objects.requireNonNull(after);

        return (a, b, c) -> {
            accept(a, b, c);
            after.accept(a, b, c);
        };
    }
}
