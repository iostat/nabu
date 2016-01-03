package io.stat.nabuproject.core.util.functional;

/**
 * Basically like BiFunction, except it takes three arguments!
 * @param <T> type of first arg
 * @param <U> type of second arg
 * @param <V> type of third arg
 * @param <R> type of return value.
 */
@FunctionalInterface
public interface TriFunction<T, U, V, R> {
    /**
     * Do something with
     * @param t arg the 1st
     * @param u arg the 2nd
     * @param v arg the 3rd
     * @return an R
     */
    R apply(T t, U u, V v);
}
