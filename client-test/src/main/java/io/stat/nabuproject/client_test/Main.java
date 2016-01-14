package io.stat.nabuproject.client_test;

import com.google.common.collect.ImmutableList;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.util.JVMHackery;
import io.stat.nabuproject.nabu.client.NabuClient;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * A test bed for the Nabu client. Don't use unless you hate yourself.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class Main {
    private final List<AddressPort> servers;
    private final NabuClient client;

    public Main() throws Exception {
        this.servers = ImmutableList.of(
                new AddressPort("127.0.0.1", 6228)
        );
        this.client = new NabuClient("elasticsearch_michaelschonfeld", servers);

        JVMHackery.addJvmSignalHandler("INT", (sigint) -> client.disconnect());
    }

    private void start() throws Exception {
        client.connect();
    }


    public static void main(String[] args) {
        try {
            Main m = new Main();
            m.start();
        } catch(Exception e) {
            logger.error("o noes!", e);
        }

        logger.info("break goes r here");
    }
}
