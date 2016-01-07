package io.stat.nabuproject.core.enkiprotocol.dispatch;

import io.stat.nabuproject.core.enkiprotocol.EnkiPacketConnection;
import io.stat.nabuproject.core.enkiprotocol.packet.EnkiPacket;
import io.stat.nabuproject.core.util.dispatch.CallbackReducerCallback;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * a {@link CallbackReducerCallback} which will ACK a packet if all callbacks
 * succeeded, or NAK it otherwise.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
public final class AckOnSuccessCRC extends CallbackReducerCallback {
    private final @Getter String name;
    private final EnkiPacketConnection cnxn;
    private final EnkiPacket packet;

    public AckOnSuccessCRC(String collectionType,
                       EnkiPacketConnection cnxn,
                       EnkiPacket packet) {
        this.name = String.format("%s-%s::%s", cnxn.prettyName(), collectionType, packet);
        this.packet = packet;
        this.cnxn = cnxn;
    }

    @Override
    public void failedWithThrowable(Throwable t) {
        logger.error("Received an Exception while collecting callbacks for packet " + name, t);
        cnxn.nak(packet);
    }

    @Override
    public void failed() {
        logger.error("Some dispatch tasks failed for " + name);
        cnxn.nak(packet);
    }

    @Override
    public void success() {
        cnxn.ack(packet);
    }
}
