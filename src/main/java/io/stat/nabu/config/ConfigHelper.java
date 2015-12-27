package io.stat.nabu.config;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Contains helper methods related to the config system
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public final class ConfigHelper {
    public static String readStreamAndPreprocess(InputStream in) throws IOException {
        String unprocessed = CharStreams.toString(new InputStreamReader(in, Charsets.UTF_8));

        Pattern pattern = Pattern.compile("\\$\\{(.+?)\\}");
        Matcher matcher = pattern.matcher(unprocessed);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String replacement = processSubstitution(matcher.group(1));
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
        }

        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private static String processSubstitution(String key) {
        String[] subParams = key.split(":");

        if(subParams.length != 2) {
            logger.error("Not sure how to perform template substitution on {}", key);
            return key;
        }
        String type = subParams[0];
        String name = subParams[1];

        switch(type) {
            case "env":
                String envVal = System.getenv(name);
                return envVal != null ? envVal : "";
            case "prop":
                return System.getProperty(name, "");
            default:
                logger.error("Not sure how to perform template substitution on {}", key);
                return key;
        }
    }
}
