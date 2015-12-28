package io.stat.nabuproject.nabu;

import io.stat.nabuproject.core.ComponentException;

/**
 * Hah! Fooled ya!
 */
public interface Nabu {
    void start() throws ComponentException;
    void shutdown() throws ComponentException;
}