package io.stat.nabuproject.core.util;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A generic immutable binary tuple.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 * @param <T> the type of the first element
 * @param <U> the type of the second element
 */
public class Tuple<T extends Serializable, U extends Serializable> implements Serializable {
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

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof Tuple)) {
            return false;
        }

        Tuple t = ((Tuple) o);
        return t.first().equals(this.first()) && t.second().equals(this.second());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(first, second);
    }

    public <NewT extends Serializable> Tuple<NewT, U> fmap1(Function<T, NewT> converter) {
        return new Tuple<>(converter.apply(first), second);
    }

    public <NewU extends Serializable> Tuple<T, NewU> fmap2(Function<U, NewU> converter) {
        return new Tuple<>(first, converter.apply(second));
    }

    public <NT extends Serializable, NU extends Serializable> Tuple<NT, NU> xform(Function<T, NT> convFst, Function<U, NU> convSnd) {
        return new Tuple<>(convFst.apply(first), convSnd.apply(second));
    }

    public static Function<Tuple<?, ?>, ?> fst = Tuple::first;
    public static Function<Tuple<?, ?>, ?> snd = Tuple::second;
}
