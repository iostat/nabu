package io.stat.nabuproject.nabu.router;

import io.stat.nabuproject.core.Component;
import io.stat.nabuproject.nabu.common.command.NabuCommand;
import io.stat.nabuproject.nabu.server.NabuCommandSource;

/**
 * Something which can take incoming {@link NabuCommand}s and respond to them
 * appropriately.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public abstract class CommandRouter extends Component {
    /**
     * Called when a command is received from a client.
     * @param src the {@link NabuCommandSource} that sent this command
     * @param command the command that was sent
     */
    public abstract void inboundCommand(NabuCommandSource src, NabuCommand command);
}
