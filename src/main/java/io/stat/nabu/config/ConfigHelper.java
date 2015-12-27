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
 * Contains helper methods related to the Config system
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public final class ConfigHelper {
    /**
     * Reads an InputStream, assuming the UTF-8 charset and preprocesses any substitutions.
     * A substitution has the format: <tt>${source:key[:fallback]}</tt>
     * Where:
     * <ul>
     *     <li><u>source</u>: a source for data to be substituted</li>
     *     <li><u>key</u>: a key to pull from the source</li>
     *     <li><u>fallback</u>: if the <tt>source</tt> does not contain <tt>key</tt>, fall back this value</li>
     * </ul>
     * and the sources currently supported are
     * <ul>
     *     <li><u>env</u>: environment variables (looked up via {@link System#getenv(String)})</li>
     *     <li><u>prop</u>: Java system properties (looked up via {@link System#getProperty(String)})</li>
     * </ul>
     *
     * @param in an InputStream containing the data to read and prepropcess
     * @return A UTF-8 String with all substitutions performed
     * @throws ConfigException if there was an error while processing substitutions
     * @throws IOException if there was an error with reading the stream
     */
    public static String readStreamAndPreprocess(InputStream in) throws ConfigException, IOException {
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

    private static String processSubstitution(String subMatch) throws ConfigException {
        String[] subSplit = subMatch.split(":");
        String source, key;
        String defaultValue = null;

        if(subSplit.length < 2 || subSplit.length > 3) {
            throw new SubstitutionException("Malformed substitution: " + subMatch);
        }

        source = subSplit[0];
        key      = subSplit[1];

        if(subSplit.length == 3) {
            defaultValue = subSplit[2];
        }

        switch(source) {
            case "env":
                String envVar = System.getenv(key);
                if(envVar == null) envVar = defaultValue;
                if(envVar == null) {
                    throw new SubstitutionException(String.format(
                            "Environment variable %s is unset and no fallback was specified (in %s)",
                            key,
                            subMatch));
                }

                return envVar;
            case "prop":
                String property = System.getProperty(key, defaultValue);
                if(property == null) {
                    throw new SubstitutionException(String.format(
                            "System Property %s is unset and no fallback was specified (in %s)",
                            key,
                            subMatch));
                }
            default:
                throw new SubstitutionException(String.format("Unsupported substitution source %s (in %s)", source, subMatch));
        }
    }
}
