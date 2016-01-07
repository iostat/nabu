package io.stat.nabuproject.core.util.functional;

import java.util.Comparator;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;

/**
 * Allows for fun and exciting composition of method references
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public final class FluentCompositions {
    @FunctionalInterface
    public interface ComposableComparator<T> extends BiFunction<T, T, Integer>, Comparator<T> {
        int compare(T a, T B);

        default Integer apply(T a, T b) {
            return compare(a, b);
        }

        default ComposableComparator<T> then(IntUnaryOperator op) {
            return (a, b) -> op.applyAsInt(compare(a, b));
        }
    }

    /**
     * Return the logical inverse of the target predicate. Identical to Predicate::negate.
     * Illustrative example:
     * <code>
     *     // filter empty strings
     *     listOfStrings.stream()
     *                  .filter(string -> !string.isEmpty)
     *                  .mapToInt(String::length)
     *                  .sum();
     *
     *     // alternatively
     *     listOfStrings.stream()
     *                  .filter(not(String.isEmpty))
     *                  .mapToInt(String::length)
     *                  .sum();
     * </code>
     * @param target the predicate to negate
     * @param <T> the type of argument the predicate takes
     * @return a predicate which returns the logical negative of <tt>target</tt>.
     */
    public static <T> Predicate<T> not(Predicate<T> target) {
        return target.negate();
    }

    /**
     * Partially apply the first argument to a BiPredicate.
     * Illustrative example (also applies to {@link FluentCompositions#curry(BiFunction, Object)}:
     *
     * <code>
     *     // FunMath has common math operations as functional predicates
     *     // instead of static methods.
     *     class {@link FunMath} {
     *         static boolean gte(long a, long b) {
     *             return a &gt;= b;
     *         }
     *
     *         static long mul(long a, long b) {
     *             return a * b;
     *         }
     *     }
     *
     *     // lambdas.. kinda fun
     *     streamOfInts.filter(a -&gt; FunMath.gte(5, a))
     *                 .map(anInt -&gt; FunMath.mul(3, anInt))
     *                 .sum();
     *
     *     // curry... delicious
     *     streamOfInts.filter(curry(FunMath::gte, 5)) // the BiPredicate&lt;T,U&gt; curry
     *                 .map(curry(FunMath::mul, 3))    // the BiFunction&lt;T, U, R&gt; curry
     *                 .sum();
     * </code>
     * @param bp the BiPredicate to partially apply <tt>t</tt> to
     * @param t the first argument to the BiPredicate
     * @param <T> the type of the first argument
     * @param <U> the type of the second argument
     * @return a Predicate which will call the original BiPredicate,
     *         with the first argument t and the second as whatever the Predicate gets
     */
    public static <T, U> Predicate<? super U> curry(BiPredicate<T, ? super U> bp, T t) {
        return u -> bp.test(t, u);
    }

    /**
     * Like {@link FluentCompositions#curry(BiPredicate, Object)}, but for a BiFunction
     * which can return Objects not limited to Booleans
     * @param bf the BiFunction to partially apply
     * @param t the first argument
     * @param <T> the type of the first argument
     * @param <U> the type of the second argument
     * @param <R> the return type of the function
     * @return a Function which calls the original BiFunction
     */
    public static <T, U, R> Function<? super U, R> curry(BiFunction<T, ? super U, R> bf, T t) {
        return u -> bf.apply(t, u);
    }

    /**
     * Partially apply the second argument to a BiPredicate.
     * Fundamentally identical to {@link FluentCompositions#curry(BiPredicate, Object)}, except
     * the <b>SECOND</b> argument gets replaced.
     * <code>
     *     import static {@link FunMath}.*;
     *
     *     // lambdas.. kinda fun
     *     streamOfInts.filter(a -&gt; lt(a, 100))     // a -&gt; a &lt; 100
     *                 .map(anInt -&gt; mul(anInt, 3)) // a -&gt; a * 3
     *                 .sum();
     *
     *     // curry... delicious
     *     streamOfInts.filter(curry2(lt, 100)) // the BiPredicate&lt;T,U&gt; curry
     *                 .map(curry2(mul, 3))     // the BiFunction&lt;T, U, R&gt; curry
     *                 .sum();
     * </code>
     * @param bp the BiPredicate
     * @param u the second argument to it
     * @param <T> the type of the first argument
     * @param <U> the type of the second argument
     * @return a Predicate that passes its arg as the first arg, and u as the second arg to the BiPredicate
     */
    public static <T, U> Predicate<T> curry2(BiPredicate<? super T, U> bp, U u) {
        return t -> bp.test(t, u);
    }

    /**
     * Composes a predicate to run after a function
     * @param f1 the function
     * @param p2 the predicate which will take f1&apos;s returned value
     * @param <T> the type of argument that f1 takes
     * @param <U> the type that f1 returns and p2 accepts
     * @return a predicate which takes applys f1 to its input, and tests the result with p2.
     */
    public static <T, U> Predicate<T> compose(Function<T, U> f1, Predicate<U> p2) {
        return t -> p2.test(f1.apply(t));
    }

    public static <T, U, R> Function<T, R> compose(Function<T, U> f1, Function<U, R> f2) {
        return f1.andThen(f2);
    }

    /**
     * Similar to {@link FluentCompositions#compose(Function, Predicate)}, but for a BiFunction to a Function
     * @param f1 the BiFunction
     * @param f2 the Function that accepts the result of f1
     * @param <T> the type of the first argument to the BiFunction
     * @param <U> the type of the second argument to the BiFunction
     * @param <V> the type that the BiFunction returns, and that the Function accepts as its argument
     * @param <R> the type that the Function returns
     * @return a BiFunction which returns the result of its arguments passed into f1, the result of which is passed into f2
     */
    public static <T, U, V, R> BiFunction<T, U, R> compose(BiFunction<T, U, V> f1, Function<V,R> f2) {
        return f1.andThen(f2);
    }

    /**
     * The cleanest way to explain it to compare it to Haskell's <tt>Data.Function.on</tt>.
     * The most intuitive way is by example.
     * Say you have a List of Tuple&lt;String, Long&gt;, and you want to sort them based on the long
     * and you want to use the Streams API. You could do with (guava's Longs.compare, btw)
     * <code>myList.stream().sorted(on(Longs::compare, Tuple::second)).collect(Collectors.toList())</code>
     * If you wanted to sort it backwards, <code>myList.stream().sorted(negate(on(Longs::compare, Tuple::second))).collect(Collectors.toList())</code>
     * It is effectively a function composition of each argument into <tt>xformer</tt> and then into a <tt>bfun</tt>
     * Example:
     * @param xformer The function which transforms type A into the type B that xmp expects
     * @param cmp The comparator which expects arguments of type B
     * @param <A> the type of original value
     * @param <B> the type that the comparator expects
     */
    public static <A, B> ComposableComparator<A> on(Function<A, B> xformer, Comparator<B> cmp) {
        return (a, b) -> cmp.compare(xformer.apply(a), xformer.apply(b));
    }

    public static <A, B> Function<A, B> hardCast(Class<B> dest) {
        return dest::cast;
    }
}

