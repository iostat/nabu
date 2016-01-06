package io.stat.nabuproject.core.util.functional;

import java.util.Comparator;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * Allows for fun and exciting composition of method references
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public final class FluentCompositions {
    public static <T> Predicate<T> not(Predicate<T> target) {
        return target.negate();
    }

    public static <T, U> Predicate<? super U> curryp(BiPredicate<T, ? super U> bp, T t) {
        return u -> bp.test(t, u);
    }
    public static <T, U, R> Function<? super U, R> curry(BiFunction<T, ? super U, R> bf, T t) {
        return u -> bf.apply(t, u);
    }

    public static <T, U> Predicate<T> curry2p(BiPredicate<T, ? super U> bp, U u) {
        return t -> bp.test(t, u);
    }

    public static <T> Predicate<T> f2p(Function<T, Boolean> f) {
        return f::apply;
    }
    public static <T, U> BiPredicate<T, U> f2p(BiFunction<T, U, Boolean> f) {
        return f::apply;
    }

    public static <T, U> Predicate<T> composep(Function<T, U> f1, Predicate<U> p2) {
        return t -> p2.test(f1.apply(t));
    }

    public static <T, U, V, R> BiFunction<T, U, R> compose(BiFunction<T, U, V> f1, Function<V,R> f2) {
        return (t, u) -> f2.apply(f1.apply(t, u));
    }

    /**
     * The cleanest way to explain it to compare it to Haskell's <tt>Data.Function.on</tt>.
     * The most intuitive way is by example.
     * Say you have a List of Tuple&lt;String, Long&gt;, and you want to sort them based on the long
     * and you want to use the Streams API. You could do with (guava's Longs.compare, btw)
     * <code>myList.stream().sorted(on(Longs::compare, Tuple::second)).collect(Collectors.toList())</code>
     * If you wanted to sort it backwards, <code>myList.stream().sorted(neg(on(Longs::compare, Tuple::second))).collect(Collectors.toList())</code>
     * It is effectively a function composition of each argument into <tt>xformer</tt> and then into a <tt>bfun</tt>
     * Example:
     * @param xformer The function which transforms type A into the type B that xmp expects
     * @param cmp The comparator which expects arguments of type B
     * @param <A> the type of original value
     * @param <B> the type that the comparator expects
     */
    public static <A, B> Comparator<A> on(Function<A, B> xformer, Comparator<B> cmp) {
        return (a, b) -> cmp.compare(xformer.apply(a), xformer.apply(b));
    }

    public static <A> Comparator<A> compose(Comparator<A> comparator, UnaryOperator<Integer> op) {
        return (a, b) -> op.apply(comparator.compare(a, b));
    }
}
