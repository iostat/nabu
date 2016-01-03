package io.stat.nabuproject.core.util.functional;

/**
 * Basically like BiFunction, except it takes 100% more arguments!
 * @param <T> type of first arg
 * @param <U> type of second arg
 * @param <V> type of third arg
 * @param <W> yes.
 * @param <R> type of return value
 * @return an R
 * @see PentaFunction for even more exasperation
 */
@FunctionalInterface
public interface QuadFunction<T, U, V, W, R> {
    /**
     * Do something with
     * @param t arg the 1st
     * @param u arg the 2nd
     * @param v arg the 3rd
     * @param w <i>[exasperation intensifies]</i>
     *
     */
    R apply(T t, U u, V v, W w);
}

