package io.stat.nabuproject.enki.leader;

import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.stat.nabuproject.Version;
import io.stat.nabuproject.core.net.AddressPort;
import io.stat.nabuproject.core.util.ProtocolHelper;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

/**
 * A simple data class that represents leader election data
 * that we can put into ZooKeeper.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor @EqualsAndHashCode(callSuper=true)
final class ZKLeaderData extends LeaderData {
    private static final long serialVersionUID = -6197344956374201028L;

    private static final byte[] MAGIC = { 0x45, 0x5A, 0x4B, 0x4C, 0x45, 0x44 };
    private static final short ZKLD_MAGIC = 0x01;
    private static final short ZKAP_MAGIC = 0x02;

    private final @Getter String path;
    private final @Getter String version;
    private final @Getter String nodeIdentifier;
    private final @Getter AddressPort addressPort;
    private final @Getter long priority;

    ZKLeaderData(String path, String nodeID, AddressPort ap, long priority) {
        this(path, Version.VERSION, nodeID, ap, priority);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("version", version)
                .add("nodeID", nodeIdentifier)
                .add("pri", priority)
                .add("ap", addressPort)
                .toString();
    }

    public String prettyPrint() {
        return String.format(
                "%s=>%d[%s/%s:%d]",
                getPath(), getPriority(),
                getNodeIdentifier(), getAddressPort().getAddress(),
                getAddressPort().getPort()
        );
    }

    public String toBase64() {
        ByteBuf buffer = Unpooled.buffer(10);

        buffer.writeBytes(MAGIC);
        ProtocolHelper.writeStringToByteBuf(getVersion(), buffer);
        ProtocolHelper.writeStringToByteBuf(getNodeIdentifier(), buffer);
        ProtocolHelper.writeStringToByteBuf(getAddressPort().getAddress(), buffer);
        buffer.writeInt(getAddressPort().getPort());

        return java.util.Base64.getEncoder().encodeToString(convertAndRelease(buffer));
    }

    @SneakyThrows
    public static ZKLeaderData fromBase64(String s, String path, long priority) {
        byte[] bytes = java.util.Base64.getDecoder().decode(s);

        for(int i = 0; i < MAGIC.length; i++) {
            if(bytes[i] != MAGIC[i]) {
                // nope
                throw new java.io.NotSerializableException("Mismatched MAGIC");
            }
        }

        ByteBuf buffer = Unpooled.wrappedBuffer(bytes);
        buffer.skipBytes(MAGIC.length);

        String version = ProtocolHelper.readStringFromByteBuf(buffer);
        if(!version.equals(Version.VERSION)) {
            // the leader data may change between nabu project versions
            // so lets just be safe and give it garbage that anybody
            // who checks leader liveness will summarily ignore on the merit
            // that this IP is unconnectable
            return new ZKLeaderData(version, "OUTDATEDNODE", new AddressPort("999.999.999.999", 99999), priority);
        }

        String nodeId  = ProtocolHelper.readStringFromByteBuf(buffer);
        String address = ProtocolHelper.readStringFromByteBuf(buffer);
        int    port    = buffer.readInt();

        return new ZKLeaderData(path, version, nodeId, new AddressPort(address, port), priority);
    }

    private static byte[] convertAndRelease(ByteBuf buffer) {
        byte[] dest = new byte[buffer.readableBytes()];
        buffer.getBytes(0, dest);
        buffer.release();

        return dest;
    }
}
