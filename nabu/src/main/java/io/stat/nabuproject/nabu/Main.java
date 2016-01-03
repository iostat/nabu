package io.stat.nabuproject.nabu;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.stat.nabuproject.Version;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.config.ConfigModule;
import io.stat.nabuproject.core.elasticsearch.ESModule;
import io.stat.nabuproject.core.enkiprotocol.EnkiClientModule;
import io.stat.nabuproject.core.util.JVMHackery;
import io.stat.nabuproject.nabu.kafka.KafkaModule;
import io.stat.nabuproject.nabu.router.RouterModule;
import io.stat.nabuproject.nabu.server.ServerModule;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

/**
 * <a href="https://www.youtube.com/watch?v=jEbj7wVjxEYL" target="_blank">Emperor + Mefjus - Void Main Void</a>
 */
@Slf4j
public class Main {
    private @Getter @Delegate Nabu nabu;
    private @Getter Injector injector;

    private Main() throws ComponentException {
        registerSignalHandlers();

        int pid = JVMHackery.getPid();
        String jvmName = System.getProperty("java.vm.name", "<java.vm.name not set>");

        logger.info("Starting Nabu v" + Version.VERSION);
        logger.info("PID {}", pid == -1 ? "<not available>" : pid);
        logger.info("JVM: {}", jvmName);
        logger.info("$PWD: {}", System.getenv("PWD"));

        this.injector = Guice.createInjector(
                new NabuModule(),
                new EnkiClientModule(),
                new ConfigModule(),
                new ESModule(),
                new KafkaModule(),
                new RouterModule(),
                new ServerModule()
        );

        nabu = injector.getInstance(Nabu.class);
    }

    /**
     * The main function is where a program starts execution, and typically has access to the command arguments
     * given to the program. It is responsible for the high-level organization of a program's functionality.
     *
     * @param args the command arguments given to the program.
     * @throws Throwable because lets be honest... who cares...
     */
    public static void main(String[] args) throws Throwable {
        new Main().start();
    }

    private void registerSignalHandlers() {
        JVMHackery.addJvmSignalHandler("INT", signal -> {
            logger.info("Received a SIGINT. Shutting down Nabu.");
            try {
                if(nabu == null) {
                    int spins = 1;
                    while(spins <= 5) {
                        logger.warn ("Nabu not fully initialized yet. Sleeping for 5 seconds (Spin {} of 5)");
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            logger.error("Received a {} while spinning for Nabu to initialize!");
                        }
                        if(nabu != null) { break; }
                        spins++;
                    }
                }
                if(nabu == null) {
                    logger.warn("Nabu not initialized after 5 spins of 5 seconds. God help us all.");
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
