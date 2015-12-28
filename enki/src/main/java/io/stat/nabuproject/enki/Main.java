package io.stat.nabuproject.enki;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.stat.nabuproject.Version;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.config.ConfigModule;
import io.stat.nabuproject.core.elasticsearch.ESModule;
import io.stat.nabuproject.core.util.JVMHackery;
import io.stat.nabuproject.enki.server.EnkiServerModule;
import lombok.extern.slf4j.Slf4j;

/**
 * <a href="https://www.youtube.com/watch?v=jEbj7wVjxEYL" target="_blank">Emperor + Mefjus - Void Main Void</a>
 */
@Slf4j
public class Main {
    private Enki enki;

    private Main() throws ComponentException {
        registerSignalHandlers();

        int pid = JVMHackery.getPid();
        String jvmName = System.getProperty("java.vm.name", "<java.vm.name not set>");

        logger.info("Starting Nabu v" + Version.VERSION);
        logger.info("PID {}", pid == -1 ? "<not available>" : pid);
        logger.info("JVM: {}", jvmName);

        Injector injector = Guice.createInjector(
                new EnkiModule(),
                new ConfigModule(),
                new ESModule(),
                new EnkiServerModule()
        );

        this.enki = injector.getInstance(Enki.class);
        enki.start();
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
                enki.shutdown();
            } catch(ComponentException e) {
                logger.warn("ComponentException thrown during shutdown!", e);
            }
        });

        JVMHackery.addJvmSignalHandler("HUP", signal ->
                logger.error("One day, this will be used to for restart-less config reloading. Or something like that")
        );
    }
}
