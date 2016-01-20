package io.stat.nabuproject.client_test;

import com.fasterxml.jackson.core.JsonFactory;
import io.stat.nabuproject.core.util.concurrent.NamedThreadPoolExecutor;
import lombok.Getter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by io on 1/15/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
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

    protected final JsonFactory jf;
    protected final ProfileMappingAcceptor pma;
    protected CountDownLatch waitForAll;

    @java.beans.ConstructorProperties({"oks", "retrs", "qs", "fails", "submitExcs", "futureExcs", "parseTime", "peformTime", "totalDocs", "jf", "pma"})
    public ProfileOperation(AtomicLong oks, AtomicLong retrs, AtomicLong qs, AtomicLong fails, AtomicLong submitExcs, AtomicLong futureExcs, AtomicLong parseTime, AtomicLong peformTime, AtomicInteger totalDocs) {
        this.oks = oks;
        this.retrs = retrs;
        this.qs = qs;
        this.fails = fails;
        this.submitExcs = submitExcs;
        this.futureExcs = futureExcs;
        this.parseTime = parseTime;
        this.peformTime = peformTime;
        this.totalDocs = totalDocs;

        jf = new JsonFactory();
        pma = new ProfileMappingAcceptor(waitForAll, futureExcs, oks, retrs, qs, fails);
    }

    public abstract void doPrepare(ForkJoinPool fjp, NamedThreadPoolExecutor executor) throws Throwable;
    public abstract void perform(NamedThreadPoolExecutor executor) throws Throwable;
}
