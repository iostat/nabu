package io.stat.nabuproject.core.util.functional;

import java.util.Objects;

/**
 * Basically like BiConsumer, except it takes 50% more arguments!
 * @param <T> type of first arg
 * @param <U> type of second arg
 * @param <V> type of third arg
 * @param <W> yes.
 *
 * @see PentaConsumer for even more exasperation
 */
@FunctionalInterface
public interface QuadConsumer<T, U, V, W> {
    /**
     * Do something with
     * @param t arg the 1st
     * @param u arg the 2nd
     * @param v arg the 3rd
     * @param w <i>[exasperation intensifies]</i>
     */
    void accept(T t, U u, V v, W w);

    /**
     * Allows you chain together two QuadConsumers to run one after the other.
     * @param after the QuadConsumer to run after this one.
     * @return a QuadConsumer that runs this consumer, followed by <tt>after</tt>
     */
    default QuadConsumer<T, U, V, W> andThen(QuadConsumer<? super T, ? super U, ? super V, ? super W> after) {
        Objects.requireNonNull(after);

        return (a, b, c, d) -> {
            accept(a, b, c, d);
            after.accept(a, b, c, d);
        };
    }
}

