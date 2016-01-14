package io.stat.nabuproject.nabu.common.command;

import io.stat.nabuproject.nabu.common.response.IDResponse;

/**
 * A command sent by the client telling the Nabu to identify
 * itself.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class IdentifyCommand extends NabuCommand {
    public IdentifyCommand(long sequence) {
        super(Type.IDENTIFY, sequence);
    }

    public final IDResponse identifyResponse(String name) {
        return new IDResponse(getSequence(), name);
    }
}
