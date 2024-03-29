package io.stat.nabuproject.client_test;

import io.stat.nabuproject.core.util.concurrent.NamedThreadPoolExecutor;
import io.stat.nabuproject.nabu.client.NabuClient;
import io.stat.nabuproject.nabu.client.NabuClientFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
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
@Slf4j
public class UpsertTest extends ProfileOperation {
    private final NabuClient client;
    private List<String> documents;

    public UpsertTest(NabuClient client, AtomicLong oks, AtomicLong retrs,
                      AtomicLong qs, AtomicLong fails, AtomicLong submitExcs,
                      AtomicLong futureExcs, AtomicLong parseTime, AtomicLong performTime,
                      AtomicInteger totalDocs) {
        super(oks, retrs, qs, fails, submitExcs, futureExcs, parseTime, performTime, totalDocs);
        this.client = client;
    }

    @Override
    public void doPrepare(ForkJoinPool fjp, NamedThreadPoolExecutor executor) throws Throwable {
        documents =
                Common.readDump(fjp, jf, Common.typeref, parseTime);

        totalDocs.set(documents.size());
        logger.info("Expecting {} docs", totalDocs);
        waitForAll = new CountDownLatch(totalDocs.get());
    }

    @Override
    public void perform(NamedThreadPoolExecutor executor) throws Throwable {
        documents.stream().forEach(doc ->
                executor.submit(() -> {
                    long start = System.nanoTime();
                    try {
                        NabuClientFuture f = client.prepareUpdateCommand("test-20-shards", "dawck")
                                .withSource(doc)
                                .shouldRefresh(true)
                                .withID(Common.randomUUID())
                                .isUpsert(true)
                                .execute();

                        f.whenComplete(pma.invoke());
                    } catch (Exception e) {
                        submitExcs.incrementAndGet();
                        waitForAll.countDown();
                    } finally {
                        long stop = System.nanoTime();
                        peformTime.addAndGet(stop - start);
                    }
                })
        );

        waitForAll.await();
    }
}
