package com.socialrank.nabu.config;

import lombok.Builder;
import lombok.Data;

/**
 * Created by io on 12/21/15. (929) 253-6977 $50/hr
 */
@Data @Builder
public class NabuConfig {
    private final String env;

    private final String listenAddress;
    private final int listenPort;

    private final int workerThreads;
}
