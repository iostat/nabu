package io.stat.nabuproject.nabu.elasticsearch;

import io.stat.nabuproject.nabu.common.command.NabuWriteCommand;

import java.util.ArrayDeque;

/**
 * Something which can take NabuWriteCommands and perform the ES
 * write action for them.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface NabuCommandESWriter {
    /**
     * Write a single NabuWriteCommand into ES
     * @param nwc the NabuWriteCommand to write
     * @return how long the execution took.
     */
    ESWriteResults singleWrite(NabuWriteCommand nwc);

    /**
     * Write a list of NabuWriteCommands into ES
     * as a BulkRequest
     * @param commands the commands to write.
     * @return how long the execution took.
     */
    ESWriteResults bulkWrite(ArrayDeque<NabuWriteCommand> commands);
}
