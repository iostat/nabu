package io.stat.nabu.bootstrap;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.stat.nabu.Version;
import io.stat.nabu.config.ConfigModule;
import io.stat.nabu.core.ComponentException;
import io.stat.nabu.core.CoreModule;
import io.stat.nabu.core.Nabu;
import io.stat.nabu.elasticsearch.ESModule;
import io.stat.nabu.kafka.KafkaModule;
import io.stat.nabu.router.RouterModule;
import io.stat.nabu.server.ServerModule;
import io.stat.nabu.util.JVMHackery;
import lombok.extern.slf4j.Slf4j;

/**
 * <a href="https://www.youtube.com/watch?v=jEbj7wVjxEYL" target="_blank">Emperor + Mefjus - Void Main Void</a>
 */
@Slf4j
public class Main {
    private Nabu nabu;

    private Main() throws ComponentException {
        registerSignalHandlers();

        int pid = JVMHackery.getPid();
        String jvmName = System.getProperty("java.vm.name", "<java.vm.name not set>");

        logger.info("Starting Nabu v" + Version.VERSION);
        logger.info("PID {}", pid == -1 ? "<not available>" : pid);
        logger.info("JVM: {}", jvmName);

        Injector injector = Guice.createInjector(
                new CoreModule(),
                new ConfigModule(),
                new ESModule(),
                new KafkaModule(),
                new RouterModule(),
                new ServerModule()
        );

        this.nabu = injector.getInstance(Nabu.class);
        nabu.start();
    }

    /**
     * The main function is where a program starts execution, and typically has access to the command arguments
     * given to the program. It is responsible for the high-level organization of a program's functionality.
     *
     * @param args the command arguments given to the program.
     * @throws Throwable because lets be honest... who cares...
     */
    public static void main(String[] args) throws Throwable {
        new Main();
    }

    private void registerSignalHandlers() {
        JVMHackery.addJvmSignalHandler("INT", signal -> {
            logger.info("Received a SIGINT. Shutting down Nabu.");
            try {
                nabu.shutdown();
            } catch(ComponentException e) {
                logger.warn("ComponentException thrown during shutdown!", e);
            }
        });

        JVMHackery.addJvmSignalHandler("HUP", signal ->
                logger.error("One day, this will be used to for restart-less config reloading. Or something like that")
        );
    }
}
