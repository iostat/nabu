package io.stat.nabuproject.core.util.functional;

import java.util.Objects;

/**
 * {@link TriConsumer} except with 66% more arguments!
 * @param <T> yep
 * @param <U> sure
 * @param <V> yeah
 * @param <W> uh huh
 * @param <X> mhm
 */
@FunctionalInterface
public interface PentaConsumer<T, U, V, W, X> {
    /**
     * Do something with
     * @param t take a guess
     * @param u yes
     * @param v very yes
     * @param w this is why people dont
     * @param x document their code
     */
    void accept(T t, U u, V v, W w, X x);

    /**
     * Allows you chain together two PentaConsumers to run one after the other.
     * @param after the consumer to run after this one.
     * @return a PentaConsumer that runs this consumer, followed by <tt>after</tt>
     */
    default PentaConsumer<T, U, V, W, X> andThen(PentaConsumer<? super T, ? super U, ? super V, ? super W, ? super X> after) {
        Objects.requireNonNull(after);

        return (a, b, c, d, e) -> {
            accept(a, b, c, d, e);
            after.accept(a, b, c, d, e);
        };
    }
}
