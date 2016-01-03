package io.stat.nabuproject.core.enkiprotocol.dispatch;

import io.stat.nabuproject.core.enkiprotocol.EnkiPacketConnection;
import io.stat.nabuproject.core.util.dispatch.CallbackReducerCallback;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by io on 1/3/16. io is an asshole because
 * he doesn't write documentation for his code.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public final class KillCnxnOnFailCRC extends CallbackReducerCallback {
    private final String name;
    private final EnkiPacketConnection cnxn;

    public KillCnxnOnFailCRC(String collectionType,
                             EnkiPacketConnection cnxn) {
        this.name = String.format("%s-%s", cnxn.prettyName(), collectionType);
        this.cnxn = cnxn;
    }

    @Override
    public void failedWithThrowable(Throwable t) {
        logger.error("Received an Exception while collecting " + name, t);
        cnxn.kill();
    }

    @Override
    public void failed() {
        logger.error("Some dispatch tasks failed for {}", name);
        cnxn.kill();
    }

    @Override
    public void success() { /* no-op */ }
}
