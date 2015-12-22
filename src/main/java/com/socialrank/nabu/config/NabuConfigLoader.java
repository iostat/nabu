package com.socialrank.nabu.config;

import lombok.val;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by io on 12/21/15. (929) 253-6977 $50/hr
 */
public final class NabuConfigLoader {
    public static final String NABU_YML_PATH = "/nabu.yml";

    private static final List<String> requiredProperties = Collections.unmodifiableList(
            new ArrayList<String>() {{
                add("nabu.env");
                add("nabu.address");
                add("nabu.port");
            }}
    );

    public static NabuConfig parseBundledConfig() throws IOException, NabuConfigException, NumberFormatException {
        Properties loadedProps = new Properties();
        InputStream nabuYmlInputStream = ClassLoader.getSystemResourceAsStream(NABU_YML_PATH);

        loadedProps.load(nabuYmlInputStream);
        nabuYmlInputStream.close();

        for(String requiredProperty : requiredProperties) {
            if (!loadedProps.containsKey(requiredProperty)) {
                throw new NabuConfigException("Required property " + requiredProperty + " not set!");
            }
        }

        val builder = NabuConfig.builder();
        builder.env(loadedProps.getProperty("nabu.env"));
        builder.listenAddress(loadedProps.getProperty("nabu.address"));
        builder.listenPort(Integer.parseInt(loadedProps.getProperty("nabu.port")));


        return builder.build();
    }
}
