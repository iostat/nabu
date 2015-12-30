package io.stat.nabuproject.core.util;

import com.google.common.collect.ImmutableList;

import java.util.stream.Collector;

/**
 * Helper methods for working with Collections
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public final class CollectionHelper {
    public static <T> Collector<T, ImmutableList.Builder<T>, ImmutableList<T>> toImmutableList() {
        return Collector.of(ImmutableList::builder,
                            ImmutableList.Builder::add,
                            (left, right) -> { left.addAll(right.build()); return left; },
                            ImmutableList.Builder::build
        );
    }
}
