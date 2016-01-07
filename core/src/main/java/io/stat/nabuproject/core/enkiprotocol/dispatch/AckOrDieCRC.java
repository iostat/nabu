package io.stat.nabuproject.core.enkiprotocol.dispatch;

import io.stat.nabuproject.core.enkiprotocol.EnkiPacketConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.util.dispatch.CallbackReducerCallback;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Acks a packet on success, or kills the connection.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public class AckOrDieCRC extends CallbackReducerCallback {
    private final @Getter String name;
    private final EnkiPacketConnection cnxn;
    private final EnkiPacket packet;

    public AckOrDieCRC(String collectionType,
                       EnkiPacketConnection cnxn,
                       EnkiPacket packet) {
        this.name = String.format("%s-%s::%s", cnxn.prettyName(), collectionType, packet);
        this.packet = packet;
        this.cnxn = cnxn;
    }

    @Override
    public void failedWithThrowable(Throwable t) {
        logger.error("AckOrDieCRC#failedWithThrowable", t);
        cnxn.killConnection();
    }

    @Override
    public void failed() {
        logger.error("AckOrDieCRC#failed {}, {}", cnxn, packet);
        cnxn.killConnection();
    }

    @Override
    public void success() {
        logger.trace("AckOrDieCRC#success {}, {}", cnxn, packet);
        cnxn.ack(packet);
    }
}
