package io.stat.nabuproject.client_test;

import io.stat.nabuproject.core.util.concurrent.NamedThreadPoolExecutor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by io on 1/15/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor
public abstract class ProfileOperation {
    protected final @Getter AtomicLong oks;
    protected final @Getter AtomicLong retrs;
    protected final @Getter AtomicLong qs;
    protected final @Getter AtomicLong fails;
    protected final @Getter AtomicLong submitExcs;
    protected final @Getter AtomicLong futureExcs;
    protected final @Getter AtomicLong parseTime;
    protected final @Getter AtomicLong peformTime;
    protected final @Getter AtomicInteger totalDocs;

    public abstract void doPrepare(ForkJoinPool fjp, NamedThreadPoolExecutor executor) throws Throwable;
    public abstract void perform(NamedThreadPoolExecutor executor) throws Throwable;
}
