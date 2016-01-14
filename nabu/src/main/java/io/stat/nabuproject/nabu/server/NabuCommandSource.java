package io.stat.nabuproject.nabu.server;

import io.stat.nabuproject.nabu.common.response.NabuResponse;

/**
 * Something which receives NabuCommands from a client, and
 * can respond back to them
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public interface NabuCommandSource {
    void respond(NabuResponse toSend);
}
