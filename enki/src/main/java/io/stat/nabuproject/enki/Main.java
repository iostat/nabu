package io.stat.nabuproject.enki;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.grapher.graphviz.GraphvizGrapher;
import com.google.inject.grapher.graphviz.GraphvizModule;
import io.stat.nabuproject.Version;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.config.ConfigModule;
import io.stat.nabuproject.core.elasticsearch.ESModule;
import io.stat.nabuproject.core.kafka.KafkaModule;
import io.stat.nabuproject.core.net.AdvertisedAddressResolverModule;
import io.stat.nabuproject.core.throttling.DynamicTPPModule;
import io.stat.nabuproject.core.util.JVMHackery;
import io.stat.nabuproject.enki.integration.IntegrationModule;
import io.stat.nabuproject.enki.leader.ZKLeaderElectionModule;
import io.stat.nabuproject.enki.server.EnkiServerModule;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.PrintWriter;

/**
 * <a href="https://www.youtube.com/watch?v=jEbj7wVjxEYL" target="_blank">Emperor + Mefjus - Void Main Void</a>
 */
@Slf4j
public class Main {
    private @Getter @Delegate Enki enki;
    private @Getter Injector injector;

    private Main(boolean doDeps) throws ComponentException {
        registerSignalHandlers();

        int pid = JVMHackery.getPid();
        String jvmName = System.getProperty("java.vm.name", "<java.vm.name not set>");

        logger.info("Starting Enki v" + Version.VERSION);
        logger.info("PID {}", pid == -1 ? "<not available>" : pid);
        logger.info("JVM: {}", jvmName);
        logger.info("$PWD: {}", System.getenv("PWD"));

        this.injector = Guice.createInjector(
                new EnkiModule(),
                new ConfigModule(),
                new AdvertisedAddressResolverModule(AdvertisedAddressResolverModule.Resolver.MOST_ACCESSIBLE),
                new ESModule(),
                new KafkaModule(),
                new ZKLeaderElectionModule(),
                new IntegrationModule(true),
                new EnkiServerModule(),
                new DynamicTPPModule(EnkiConfig.class)
        );

        if(doDeps) {
            String dotDestination = "/tmp/enki-guice.dot";
            try {
                PrintWriter out = new PrintWriter(new File(dotDestination), "UTF-8");
                Injector gvInjector = Guice.createInjector(new GraphvizModule());
                GraphvizGrapher grapher = gvInjector.getInstance(GraphvizGrapher.class);
                grapher.setOut(out);
                grapher.setRankdir("TB");
                grapher.graph(injector);
                logger.info("Dumped Guice dependency graph to {}", dotDestination);
            } catch(Exception e) {
                logger.warn("Started with --dump-deps but could not dump Guice .dot graph to " + dotDestination, e);
            }
        }

        this.enki = injector.getInstance(Enki.class);
    }

    /**
     * The main function is where a program starts execution, and typically has access to the command arguments
     * given to the program. It is responsible for the high-level organization of a program's functionality.
     *
     * @param args the command arguments given to the program.
     * @throws Throwable because lets be honest... who cares...
     */
    public static void main(String[] args) throws Throwable {
        new Main(args.length > 0 && args[0].equals("--dump-deps")).start();
    }

    private void registerSignalHandlers() {
        JVMHackery.addJvmSignalHandler("INT", signal -> {
            logger.info("Received a SIGINT. Shutting down Enki.");
            try {
                if(enki == null) {
                    int spins = 1;
                    while(spins <= 5) {
                        logger.warn ("Enki not fully initialized yet. Sleeping for 5 seconds (Spin {} of 5)", spins);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            logger.error("Received a {} while spinning for Enki to initialize!");
                        }
                        if(enki != null) { break; }
                        spins++;
                    }
                }
                if(enki == null) {
                    logger.error("Enki not initialized after 5 spins of 5 seconds. God help us all.");
                } else {
                    this.shutdown();
                }
            } catch(ComponentException e) {
                logger.warn("ComponentException thrown during shutdown!", e);
            }
        });

        JVMHackery.addJvmSignalHandler("HUP", signal ->
                logger.error("One day, this will be used to for restart-less config reloading. Or something like that")
        );
    }
}
