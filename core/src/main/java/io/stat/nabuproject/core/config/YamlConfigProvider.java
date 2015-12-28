package io.stat.nabuproject.core.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.stat.nabuproject.core.ComponentException;
import io.stat.nabuproject.core.util.StreamHelper;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;

/**
 * A {@link ConfigurationProvider} that gets its data from a YAML file.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class YamlConfigProvider implements ConfigurationProvider {

    /**
     * The Map generated from the configuration file.
     */
    private Map<String, Object> yamlAsMap;

    @SuppressWarnings("unchecked")
    @Inject
    YamlConfigProvider(@Named("Configuration File Name") String configFileName) {
        try {
            Yaml yaml = new Yaml();
            String processedYaml = ConfigHelper.readStreamAndPreprocess(StreamHelper.findResource(configFileName));
            yamlAsMap = ImmutableMap.copyOf((Map<String, Object>)yaml.load(processedYaml));

        } catch(ConfigException | IOException e) {
            throw new ComponentException(true, e);
        }
    }

    @Override
    public <T> T getProperty(String key, Class<T> klass) throws ConfigException {
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
    public <T> List<T> getSequence(String key, Class<T> klass) throws ConfigException {
        Object needle = getKey(key);
        List<T> ret;

        if(List.class.isAssignableFrom(needle.getClass())) {
            return ImmutableList.copyOf((List<T>)needle);
        } else {
            throw new InvalidValueException(key + " is supposed to be a sequence!");
        }
    }

    @Override @SuppressWarnings("unchecked")
    public Map<String, Object> getSubmap(String key) throws ConfigException {
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
