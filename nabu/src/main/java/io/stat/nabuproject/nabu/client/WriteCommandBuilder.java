package io.stat.nabuproject.nabu.client;

import io.stat.nabuproject.nabu.common.command.NabuWriteCommand;

/**
 * Represents an object which can be used to
 * build {@link NabuWriteCommand}s
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
interface WriteCommandBuilder {
    /**
     * Finalize this builder by assigning it a sequence number
     * @param sequence the sequence number to assign.
     * @return a NabuWriteCommand of the type this builder creates.
     */
    NabuWriteCommand build(long sequence);

    /**
     * If an executor was specified when this WriteCommandBuilder was created, use it
     * to dispatch the command. Otherwise, throw an IllegalStateException.
     * @return a {@link NabuClientFuture} that will be fullfilled when a response is available.
     * @throws NabuClientDisconnectedException if the executor was not connected to a Nabu.
     * @throws IllegalStateException if this WriteCommandBuilder was instantiated without an executor.
     */
    NabuClientFuture execute() throws NabuClientDisconnectedException;
}
