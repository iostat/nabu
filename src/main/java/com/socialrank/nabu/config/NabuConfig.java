package com.socialrank.nabu.config;

import com.socialrank.nabu.bootstrap.NabuConfigLoader;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.NonFinal;

/**
 * Created by io on 12/21/15. (929) 253-6977 $50/hr
 */
@Value
@Builder
public class NabuConfig {
    String env;
    String listenAddress;
    int listenPort;
    int acceptorThreads;
    int workerThreads;

    @NonFinal private static NabuConfig _instance;
    @NonFinal private static boolean _initialized;

    public static NabuConfig getNabuConfig() {
        if(!_initialized) { throw new IllegalStateException("NabuConfig.bootstrap() has not been called yet!"); }
        return _instance;
    }

    public static void bootstrap() throws NabuConfigException {
        if(_initialized) { throw new IllegalStateException(); }

        _instance = NabuConfigLoader.parseBundledConfig();
        _initialized = true;
    }
}
