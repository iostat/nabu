package com.socialrank.nabu.bootstrap;

import com.socialrank.nabu.config.NabuConfig;
import com.socialrank.nabu.config.NabuConfigLoader;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by io on 12/21/15. (929) 253-6977 $50/hr
 */
@Slf4j
public class Main {
    public static void main(String[] args) {
        NabuConfig nabuConfig;

        try {
            nabuConfig = NabuConfigLoader.parseBundledConfig();
        } catch(Exception e) {

        }

    }
}
