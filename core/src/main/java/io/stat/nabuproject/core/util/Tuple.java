package io.stat.nabuproject.core.util;

import com.google.common.base.MoreObjects;
import lombok.EqualsAndHashCode;

/**
 * A generic immutable binary tuple.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 * @param <T> the type of the first element
 * @param <U> the type of the second element
 */
@EqualsAndHashCode
public class Tuple<T, U> {
    private final T first;
    private final U second;

    /**
     * @return the first element of the pair
     */
    public T first() {
        return first;
    }

    /**
     * @return the second element of the pair
     */
    public U second() {
        return second;
    }

    /**
     * Create a Tuple
     * @param first the value of the first element
     * @param second the value of the second element
     */
    public Tuple(T first, U second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public String toString(){
        return MoreObjects.toStringHelper(this)
                .add("first", first)
                .add("second", second)
                .toString();
    }
}
