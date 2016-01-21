package io.stat.nabuproject.client_test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.util.concurrent.NamedThreadPoolExecutor;
import io.stat.nabuproject.nabu.client.NabuClient;
import io.stat.nabuproject.nabu.common.response.NabuResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.FileWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A test bed for the Nabu client. Don't use unless you hate yourself.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class Main {
    private final AddressPort server;
    private final NabuClient client;
    public static final int FJP_SIZE = 2500;
    public static final int TP_SIZE = 1500;
    public static final AtomicBoolean plzStop = new AtomicBoolean(false);
    public static final AtomicBoolean reallyStop = new AtomicBoolean(false);
    public static final AddressPort LOCALHOST = new AddressPort("localhost", 6228);

    public static List<String> sources;

    public Main(int port) throws Exception {
        this.server = LOCALHOST;//new AddressPort("nabu.default.svc.srnk.int", port);
        this.client = new NabuClient("elasticsearch_michaelschonfeld", server);//new NabuClient("es.socialrank.azure", server);
    }

    private void start() throws Throwable {
        logger.info("GET IN");
        Thread.sleep(5000);
        logger.info("TOO LATE LOLZ");

        client.connect();

//        preloadedProfile();
        streamedPipe();

        client.disconnect();
    }

    private void stripSource() throws Throwable {
        JsonFactory jf = new JsonFactory();
        Files.walk(Paths.get("/Users/io/srdump"))
                .filter(Files::isRegularFile)
                .parallel()
                .forEach(path -> {
                    try {
                        String thepath = "/Users/io/sr-dump-stripped/" + path.toFile().getName();
                        JsonParser jp  = jf.createParser(path.toFile());
                        ObjectMapper om = new ObjectMapper(jf);
                        MappingIterator<Map<String, Object>> mit = om.readValues(jp, Common.typeref);
                        String ret = om.writeValueAsString(mit.next().get("_source"));

                        jp.close();

                        FileWriter fw = new FileWriter(thepath);
                        fw.write(ret);
                        fw.flush();
                        fw.close();

                        logger.info("Stripped out {}", thepath);
                    } catch (Exception e) {
                        logger.error("lolz (in {})", path, e);
                    }
                });
    }

    public void streamedPipe() throws Throwable {
        JsonFactory jf = new JsonFactory();
        AtomicBoolean shouldQuit = new AtomicBoolean(false);

        while(!plzStop.get()) {
            for(String src : sources) {
                if(shouldQuit.get()) {
                    break;
                }
                try {
                    client.prepareIndexCommand("test-20-shards", "dawck")
                            .withSource(src)
                            .shouldRefresh(true)
                            .execute()
                            .whenComplete((a, b) -> {
                                if(shouldQuit.get()) { return; }
                                if (b != null || !(a.getType() == NabuResponse.Type.QUEUED || a.getType() == NabuResponse.Type.OK)) {
                                    logger.error("{} {}", a, b);
                                    shouldQuit.set(true);
                                }
                            });
                } catch(Throwable t) {
                    logger.error("", t);
                    shouldQuit.set(true);
                } finally {
                    if(shouldQuit.get()) {
                        plzStop.set(true);
                    }
                }
            }
            System.out.println("gimme da loop, gimme da loop");
        }
    }

    public void preloadedProfile() throws Throwable {
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

        executor.shutdown();
    }

    public static void main(String[] args) throws Throwable {
        sources = Files.walk(Paths.get("/Users/io/sr-dump-stripped"))
                .filter(Files::isRegularFile)
                .limit(20000)
                .parallel()
                .map(path -> {
                    try {
                        return new String(Files.readAllBytes(path), Charset.defaultCharset());
                    } catch (Exception e) {
                        logger.error("lolz (in {})", path, e);
                        return Common.RANDO;
                    }
                })
                .collect(Collectors.toList());

        logger.info("loaded.... looping!");
        while(!reallyStop.get()) {
            int port = 6228;
            if(args.length != 0) {
                port = Integer.parseInt(args[0]);
            }
            try {
                Main m = new Main(port);
                m.start();
//            m.stripSource();
            } catch(Throwable e) {
                logger.error("o noes!", e);
            }
        }

        logger.info("thread main is done");
    }
}
