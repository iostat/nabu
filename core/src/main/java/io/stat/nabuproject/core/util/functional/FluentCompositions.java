package io.stat.nabuproject.core.util.functional;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;
import java.util.stream.Stream;

/**
 * Allows for fun and exciting composition of method references
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public final class FluentCompositions {
    @FunctionalInterface
    public interface ComposableComparator<T> extends BiFunction<T, T, Integer>, Comparator<T>, ToIntBiFunction<T, T> {
        int compare(T a, T B);

        default int applyAsInt(T a, T b) { return compare(a, b); }

        default Integer apply(T a, T b) {
            return compare(a, b);
        }

        default ComposableComparator<T> then(IntUnaryOperator op) {
            return (a, b) -> op.applyAsInt(compare(a, b));
        }
    }

    /**
     * Return the logical inverse of the target predicate. Identical to Predicate::negate.
     *
     *
     * Illustrative example:<br>
     * <pre>
     * {@code
     *     // filter empty strings
     *     listOfStrings.stream()
     *                  .filter(string -> !string.isEmpty)
     *                  .mapToInt(String::length)
     *                  .sum();
     *
     *     // alternatively
     *     listOfStrings.stream()
     *                  .filter(not(String::isEmpty))
     *                  .mapToInt(String::length)
     *                  .sum();
     * }
     * </pre>
     * @param target the predicate to negate
     * @param <T> the type of argument the predicate takes
     * @return a predicate which returns the logical negative of <tt>target</tt>.
     */
    public static <T> Predicate<T> not(Predicate<T> target) {
        return target.negate();
    }

    /**
     * Returns a curried Supplier, perfect for making a supplier that returns new instances
     * with the same parameter.
     * @param func the function to transform into a supplier
     * @param arg the argument to the function
     * @param <T> the type of argument the function takes
     * @param <R> the type of result the function returns
     * @return a function that returns the result of func with arg.
     */
    public static <T, R> Supplier<R> curry(Function<T, R> func, T arg) {
        return () -> func.apply(arg);
    }

    /**
     * The identity function for Function&lt;&gt;s
     * @return func
     */
    public static <T, R> Function<T, R> $(Function<T, R> func) {
        return func;
    }

    /**
     * The identity function for Predicate&lt;&gt;s
     * @return p
     */
    public static <T> Predicate<T> $(Predicate<T> p) {
        return p;
    }

    /**
     * The identity function for BiFunction&lt;&gt;s
     * @return func
     */
    public static <T, U, R> BiFunction<T, U, R> $(BiFunction<T, U, R> func) {
        return func;
    }

    /**
     * The most generic identity function.
     * @param <T> the type of parameter going in
     * @return a function that returns the value given to it
     */
    public static <T> Function<T, T> $() {
        return t -> t;
    }

    /**
     * Shorthand for {@link FluentCompositions#compose(Function, Predicate)}
     */
    public static <T, U> Predicate<T> $_$(Function<T, U> f, Predicate<U> p) { return compose(f, p); }

    /**
     * shorthand for {@link FluentCompositions#compose(Function, Function)}
     */
    public static <T, U, R> Function<T, R> $_$(Function<T, U> f1, Function<U, R> f2) { return compose(f1, f2); }

    /**
     * shorthand for {@link FluentCompositions#compose(BiFunction, Function)}
     */
    public static <T, U, V, R> BiFunction<T, U, R> $_$(BiFunction<T, U, V> f1, Function<V,R> f2) {
        return compose(f1, f2);
    }

    /**
     * Partially apply the first argument to a BiPredicate.
     *
     * <br>
     * Illustrative example (also applies to {@link FluentCompositions#curry(BiFunction, Object)}:<br>
     * <pre>
     * {@code
     *     // FunMath has common math operations as functional predicates
     *     // instead of static methods.
     *     class {@link FunMath} {
     *         static boolean gte(long a, long b) {
     *             return a >= b;
     *         }
     *
     *         static long mul(long a, long b) {
     *             return a * b;
     *         }
     *     }
     *
     *     // lambdas.. kinda fun
     *     streamOfInts.filter(a -> FunMath.gte(5, a))
     *                 .map(anInt -> FunMath.mul(3, anInt))
     *                 .sum();
     *
     *     // curry... delicious
     *     streamOfInts.filter(curry(FunMath::gte, 5)) // the BiPredicate<T,U> curry
     *                 .map(curry(FunMath::mul, 3))    // the BiFunction<T, U, R> curry
     *                 .sum();
     * }
     * </pre>
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
    public static <T, U, R> Function<U, R> curry(BiFunction<T, ? super U, R> bf, T t) {
        return u -> bf.apply(t, u);
    }

    /**
     * Partially apply the second argument to a BiPredicate.
     * Fundamentally identical to {@link FluentCompositions#curry(BiPredicate, Object)}, except
     * the <b>SECOND</b> argument gets replaced.<br>
     * <pre>
     * {@code
     *     import static {@link FunMath}.*;
     *
     *     // lambdas.. kinda fun
     *     streamOfInts.filter(a -> lt(a, 100))     // a -> a < 100
     *                 .map(anInt -> mul(anInt, 3)) // a -> a * 3
     *                 .sum();
     *
     *     // curry... delicious
     *     streamOfInts.filter(curry2(lt, 100)) // the BiPredicate<T,U> curry
     *                 .map(curry2(mul, 3))     // the BiFunction<T, U, R> curry
     *                 .sum();
     *
     * }
     * </pre>
     * @param bp the BiPredicate
     * @param u the second argument to it
     * @param <T> the type of the first argument
     * @param <U> the type of the second argument
     * @return a Predicate that passes its arg as the first arg, and u as the second arg to the BiPredicate
     */
    public static <T, U> Predicate<T> curry2(BiPredicate<T, ? super U> bp, U u) {
        return t -> bp.test(t, u);
    }

    /**
     * The BiFunction equivalent of {@link FluentCompositions#curry2(BiPredicate, Object)}
     */
    public static <T, U, R> Function<T, R> curry2(BiFunction<? super T, U, R> bf, U u) {
        return t -> bf.apply(t, u);
    }

    /**
     * Composes a predicate to run after a function
     * @param f1 the function
     * @param p2 the predicate which will take f1's returned value
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
     * Say you have a List of {@code Tuple<String, Long>}, and you want to sort them based on the long
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

    /**
     * Allows you to bridge a BiFunction which takes two arguments of any type to a
     * caller that only supplies one argument, by specifying two transformers to transform
     * the original single argument into two. This is very similar to one, but more flexible, as it's not
     * using what's effectively a single lens into a type to compare.
     *
     * @param arg1x The transformer which takes {@literal <A>} and generates the first parameter
     * @param arg2x The transformer which takes {@literal <B>} and generates the first parameter
     * @param original the function that's being bridged to
     * @param <A> the type of incoming argument
     * @param <B> the type of the first argument that the original function takes
     * @param <C> the type of the second argument that the original function takes
     * @param <D> the type that the original function returns
     * @return {@code (a) -> original(arg1x(a), arg2x(a)); }
     */
    public static <A, B, C, D> Function<A, D> bridge(Function<A, B> arg1x, Function<A, C> arg2x, BiFunction<B, C, D> original) {
        return (a1) -> original.apply(arg1x.apply(a1), arg2x.apply(a1));
    }

    /**
     * Apply a function to a single value.
     * @param functor the function to run
     * @param t the value to apply the functor to
     * @param <T> the type that functor accepts
     * @param <R> the type that functor returns
     * @return functor.apply(t)
     */
    public static <T, R> R fmap(Function<T, R> functor, T t) {
        return functor.apply(t);
    }

    /**
     * Apply a function to a value in the Optional monad
     * @param f the fucntion
     * @param t the Optional
     * @param <T> the type the function accepts and which is in the Optional
     * @param <R> the type that the function returns and which the new Optional will box
     * @return t.map(f);
     */
    public static <T, R> Optional<R> fmap(Function<T, R> f, Optional<T> t) {
        return t.map(f);
    }

    /**
     * Returns a Stream with the results of functor applied to each element of collection.
     * @param functor the function to apply
     * @param collection the collection to fmap over
     * @param <T> the type of objects in the original list
     * @param <R> the type of objects in the new list
     * @return a new Stream, identical to doing collection.stream().map(functor)
     */
    public static <T, R> Stream<R> fmap(Function<T, R> functor, Collection<T> collection) {
        return collection.stream().map(functor);
    }
}

