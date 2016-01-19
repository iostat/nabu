package io.stat.nabuproject.client_test;

import com.google.common.collect.ImmutableList;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.util.JVMHackery;
import io.stat.nabuproject.core.util.concurrent.NamedThreadPoolExecutor;
import io.stat.nabuproject.nabu.client.NabuClient;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A test bed for the Nabu client. Don't use unless you hate yourself.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class Main {
    private final List<AddressPort> servers;
    private final NabuClient client;
    public static final int FJP_SIZE = 100;
    public static final int TP_SIZE = 1500;

    public Main(int port) throws Exception {
        this.servers = ImmutableList.of(
                new AddressPort("nabu.default.svc.srnk.int", port)
        );
        this.client = new NabuClient("elasticsearch_michaelschonfeld", servers);

        JVMHackery.addJvmSignalHandler("INT", (sigint) -> client.disconnect());
    }

    private void start() throws Throwable {
        logger.info("GET IN");
        Thread.sleep(5000);
        logger.info("TOO LATE LOLZ");

        client.connect();

        AtomicLong oks = new AtomicLong(0);
        AtomicLong retrs = new AtomicLong(0);
        AtomicLong qs = new AtomicLong(0);
        AtomicLong fails = new AtomicLong(0);
        AtomicLong submitExcs = new AtomicLong(0);
        AtomicLong futureExcs = new AtomicLong(0);
        AtomicLong parseTime = new AtomicLong(0);
        AtomicLong performTime = new AtomicLong(0);
        AtomicInteger totalDocs = new AtomicInteger(0);

        ProfileOperation it = new UpsertTest(client, oks, retrs, qs, fails, submitExcs, futureExcs, parseTime, performTime, totalDocs);

        // 100 threads max for our parallel loader stream...
        // 1500 thread writer pool ;)
        ForkJoinPool fjp = new ForkJoinPool(FJP_SIZE);
        NamedThreadPoolExecutor executor = new NamedThreadPoolExecutor("Perform Pool", TP_SIZE, TP_SIZE, 1, TimeUnit.MINUTES, new ArrayBlockingQueue<>(120000, true));

        it.doPrepare(fjp, executor);

        fjp.shutdownNow();
        long start = System.nanoTime();

        it.perform(executor);


        logger.info("\n" +
                "{} total docs\n" +
                "{} thread pool\n" +
                "{} nanos parse time, FJP submission notwithstanding (ie prior to run)\n" +
                "{} nanos run time, thread pool submission notwithstanding\n" +
                "{} OK\n" +
                "{} FAIL\n" +
                "{} QUEUED\n" +
                "{} RETRY\n" +
                "{} Exceptions in future\n" +
                "{} Exceptions in submission\n",
                totalDocs, TP_SIZE, parseTime, performTime,
                oks, fails, qs, retrs,
                futureExcs, submitExcs);

        client.disconnect();
        executor.shutdown();
    }


    public static void main(String[] args) {
        int port = 6228;
        if(args.length != 0) {
            port = Integer.parseInt(args[0]);
        }
        try {
            Main m = new Main(port);
            m.start();
        } catch(Throwable e) {
            logger.error("o noes!", e);
        }

        logger.info("thread main is done");
    }
}
