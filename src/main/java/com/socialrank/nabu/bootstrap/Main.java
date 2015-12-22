package com.socialrank.nabu.bootstrap;

import com.socialrank.nabu.Version;
import com.socialrank.nabu.config.NabuConfig;
import com.socialrank.nabu.config.NabuConfigException;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by io on 12/21/15. (929) 253-6977 $50/hr
 */
@Slf4j
public class Main {
    public static final int LOAD_CONFIG_FAILED_EXIT = 255;
    public static final int START_NABU_SERVER_FAILED_EXIT = 220;

    public static void main(String[] args) {
        logger.info("Starting Nabu v" + Version.VERSION);

        try {
            NabuConfig.bootstrap();
        } catch(NabuConfigException e) {
            logger.error("Could not load config.", e);
            System.exit(LOAD_CONFIG_FAILED_EXIT);
        }

        try {
            new NabuServerBootstrap().bootstrapAndStart();
        } catch(InterruptedException ie) {
            logger.error("Got an InterruptedException while starting the Nabu server.", ie);
            System.exit(START_NABU_SERVER_FAILED_EXIT);
        }
    }
}
