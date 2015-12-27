package io.stat.nabu.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.stat.nabu.core.Component;
import io.stat.nabu.core.ComponentException;
import io.stat.nabu.util.StreamHelper;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;

/**
 * A {@link Component} which provides configuration to Nabu.
 *
 * Honestly this whole thing is a bit of a huge hack.
 * But I want it provided as a service so you can do shit like runtime configuration reloading
 * without having a heavy dependency like Archaius or something like that.
 *
 * Created by io on 12/21/15. (929) 253-6977 $50/hr
 */
@Slf4j
public class YamlConfigProvider extends ConfigurationProvider {
    private static final String NABU_YML_PATH = "nabu.yml";

    /**
     * The Map generated from the configuration file.
     */
    private Map<String, Object> yamlAsMap;

    @SuppressWarnings("unchecked")
    YamlConfigProvider() {
        try {
            Yaml yaml = new Yaml();
            String processedYaml = ConfigHelper.readStreamAndPreprocess(StreamHelper.findResource(NABU_YML_PATH));
            yamlAsMap = ImmutableMap.copyOf((Map<String, Object>)yaml.load(processedYaml));

        } catch(IOException e) {
            throw new ComponentException(true, e);
        }

        setupComplete();
    }

    @Override
    protected <T> T getProperty(String key, Class<T> klass) throws ConfigException {
        Object needle = getKey(key);
        T ret;

        try {
            ret = klass.cast(needle);
        } catch(ClassCastException ce) {
            throw new InvalidValueException(ce);
        }

        return ret;
    }

    @Override @SuppressWarnings("unchecked")
    protected <T> List<T> getSequence(String key, Class<T> klass) throws ConfigException {
        Object needle = getKey(key);
        List<T> ret;

        if(List.class.isAssignableFrom(needle.getClass())) {
            return ImmutableList.copyOf((List<T>)needle);
        } else {
            throw new InvalidValueException(key + " is supposed to be a sequence!");
        }
    }

    @Override @SuppressWarnings("unchecked")
    protected Map<String, Object> getSubmap(String key) throws ConfigException {
        Object needle = getKey(key);

        if(Map.class.isAssignableFrom(needle.getClass())) {
            return (Map)needle;
        } else {
            throw new InvalidValueException(key + " does not appear to be a Map");
        }
    }

    @Synchronized
    private Object getKey(String key) throws ConfigException {
        String[] keyParts = key.split("\\.");
        ArrayDeque<String> q = new ArrayDeque<>(keyParts.length);
        for(String part : keyParts) { q.addLast(part); }

        return traverseMaps(yamlAsMap, q, "");
    }

    private Object traverseMaps(Map<String, Object> thisNode,
                                ArrayDeque<String> queue,
                                String previousChain) throws ConfigException {

        String thisKey = queue.removeFirst();
        String thisChain = previousChain + (previousChain.isEmpty() ? "" : ".") + thisKey;

        if(!thisNode.containsKey(thisKey)) {
            throw new MissingPropertyException(thisChain + " does not exist!");
        }

        if(queue.size() == 0) {
            return thisNode.get(thisKey);
        } else {
            Object nextNode = thisNode.get(thisKey);
            if(!Map.class.isAssignableFrom(nextNode.getClass())) {
                throw new InvalidValueException(thisChain + " is supposed to be a Map but is a "
                        + nextNode.getClass().getCanonicalName() + " instead");
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> nextNodeCasted = (Map)nextNode;

                return traverseMaps(nextNodeCasted, queue, thisChain);
            }
        }
    }
}
