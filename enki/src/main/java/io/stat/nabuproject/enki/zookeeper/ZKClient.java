package io.stat.nabuproject.enki.zookeeper;

import io.stat.nabuproject.core.Component;

import java.util.List;

/**
 * The abstract implementation of the Zookeeper Component.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class ZKClient extends Component implements ZKEventSource {
    /**
     * Gets the contents of <tt>path</tt>
     * @param path the path
     * @return the contents of the path if it exists and has data, or null otherwise
     */
    public abstract String get(String path);

    /**
     * Gets the children of <tt>path</tt>
     * @param path the path whose childrne to get
     * @return a List of the contents of <tt>path</tt>
     */
    public abstract List<String> getChildren(String path);

    /**
     * Write data to a node in path. The node has to exist beforehand.
     * @param path the path to write to
     * @param data the data to write
     */
    public abstract void write(String path, String data);

    /**
     * Create a (persistent) node with data at path.
     * @param path the path to create
     * @param data the data to populate it with.
     */
    public abstract void create(String path, String data);
}
