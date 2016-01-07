package io.stat.nabuproject.core.util.functional;

import com.google.common.primitives.Longs;

import java.util.Comparator;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.IntUnaryOperator;

/**
 * Functional math
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public final class FunMath {
    private static long defaultNumber(Long n) {
        return n == null ? 0 : n;
    }

    public static final BiPredicate<Long, Long> gt  = (a, b) -> a > b;
    public static final BiPredicate<Long, Long> gte = (a, b) -> a >= b;
    public static final BiPredicate<Long, Long> lt  = (a, b) -> a < b;
    public static final BiPredicate<Long, Long> lte = (a, b) -> a <= b;
    public static final BiPredicate<Long, Long> eq  = (a, b) -> defaultNumber(a) == defaultNumber(b);
    public static final BinaryOperator<Long>    mul = (a, b) -> a * b;
    public static final BinaryOperator<Long>    div = (a, b) -> a / b;
    public static final BinaryOperator<Long>    add = (a, b) -> a + b;
    public static final BinaryOperator<Long>    sub = (a, b) -> a - b;
    public static final BinaryOperator<Long>    mod = (a, b) -> a % b;

    public static final IntUnaryOperator negate = (a) -> a * -1;
    public static final Comparator<Long> compare = Longs::compare;

    public static final BiPredicate<Object, Object> oeq = (a, b) -> a.equals(b);
}
