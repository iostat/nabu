package io.stat.nabuproject.client_test;

import io.stat.nabuproject.nabu.common.response.NabuResponse;
import lombok.Getter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Created by io on 1/19/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class ProfileMappingAcceptor {
    private final @Getter CountDownLatch waitForAll;
    private final @Getter AtomicLong futureExcs;
    private final @Getter AtomicLong oks;
    private final @Getter AtomicLong retrs;
    private final @Getter AtomicLong qs;
    private final @Getter AtomicLong fails;

    public ProfileMappingAcceptor(CountDownLatch waitForAll, AtomicLong futureExcs, AtomicLong oks, AtomicLong retrs, AtomicLong qs, AtomicLong fails) {
        this.waitForAll = waitForAll;
        this.futureExcs = futureExcs;
        this.oks = oks;
        this.retrs = retrs;
        this.qs = qs;
        this.fails = fails;
    }

    public BiConsumer<NabuResponse, Throwable> invoke() {
        return (resp, thrown) -> {
            waitForAll.countDown();
            if (thrown != null) {
                futureExcs.incrementAndGet();
            } else {
                switch (resp.getType()) {
                    case OK:
                        oks.incrementAndGet();
                        break;
                    case RETRY:
                        retrs.incrementAndGet();
                        break;
                    case QUEUED:
                        qs.incrementAndGet();
                        break;
                    case FAIL:
                        fails.incrementAndGet();
                        break;
                }
            }
        };
    }
}
