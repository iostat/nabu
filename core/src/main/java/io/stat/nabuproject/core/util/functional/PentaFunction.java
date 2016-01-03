package io.stat.nabuproject.core.util.functional;

/**
 * {@link TriFunction} except with 66% more arguments!
 * @param <T> yep
 * @param <U> sure
 * @param <V> yeah
 * @param <W> uh huh
 * @param <X> mhm
 * @param <R> i'm sure you get it at this point
 */
@FunctionalInterface
public interface PentaFunction<T, U, V, W, X, R> {
    /**
     * Do something with
     * @param t take a guess
     * @param u yes
     * @param v very yes
     * @param w this is why people dont
     * @param x document their code
     * @return seriously, just look at the damn signature
     */
    R apply(T t, U u, V v, W w, X x);
}
