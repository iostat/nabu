package io.stat.nabuproject.nabu.common.command;

import io.netty.buffer.ByteBuf;
import io.stat.nabuproject.core.net.ProtocolHelper;
import lombok.Getter;
import lombok.NonNull;

/**
 * An abstract NabuCommand that effectively represents a request by
 * a client for Nabu to perform work.
 */
public abstract class NabuWriteCommand extends NabuCommand {
    private final @Getter String index;
    private final @Getter String documentType;
    private final @Getter String documentID;
    private final @Getter String documentSource;
    private final boolean shouldRefresh;
    private final boolean shouldForceWrite;

    protected NabuWriteCommand(@NonNull NabuCommand.Type type,
                               long sequence,
                               @NonNull String index,
                               @NonNull String documentType,
                               @NonNull String documentID,
                               @NonNull String documentSource,
                               boolean shouldRefresh,
                               boolean shouldForceWrite) {
        super(type, sequence);
        this.index = index;
        this.documentType = documentType;
        this.documentID = documentID;
        this.documentSource = documentSource;
        this.shouldRefresh = shouldRefresh;
        this.shouldForceWrite = shouldForceWrite;
    }

    public boolean shouldRefresh() {
        return shouldRefresh;
    }
    public boolean shouldForceWrite() { return shouldForceWrite; }

    public final void encode(ByteBuf out) throws Exception {
        // first write everything except MAGIC, Type, and length
        // we can assume we'll have at least 18 bytes,
        // 1 byte shouldRefresh, 1 byte shouldForceWrite, and 4 INT lengths for index doctype id and source
        // so lets round it up to 32^H^H64 for memory alignment magic
        // (at least i think thats how it should work) ¯\_(ツ)_/¯
        ByteBuf restOfPacket = ProtocolHelper.POOLED_BYTEBUF_ALLOCATOR.buffer(64);

        restOfPacket.writeBoolean(shouldRefresh);
        restOfPacket.writeBoolean(shouldForceWrite);
        ProtocolHelper.writeStringToByteBuf(getIndex(), restOfPacket);
        ProtocolHelper.writeStringToByteBuf(getDocumentType(), restOfPacket);
        ProtocolHelper.writeStringToByteBuf(getDocumentID(), restOfPacket);
        ProtocolHelper.writeStringToByteBuf(getDocumentSource(), restOfPacket);

        if(hasEncodableProperties()) {
            encodeSpecificProperties(restOfPacket);
        }

        encodeHeader(out);
        out.writeInt(restOfPacket.readableBytes());
        out.writeBytes(restOfPacket);

        restOfPacket.release();
    }

    /**
     * Called by {@link NabuWriteCommand#encode(ByteBuf)} to determine whether or not
     * {@link NabuWriteCommand#encodeSpecificProperties(ByteBuf)} should be called
     * @return whether or not this packet needs to
     */
    protected abstract boolean hasEncodableProperties();

    /**
     * Inheritors should provide a way to serialize their own data that's not inherited
     * from the abstract NabuWriteCommand. This method is automatically called by encode
     * @param out the ByteBuf to write to. it is not necessary to write any length fields
     *            as {@link NabuWriteCommand#encode(ByteBuf)} will do that for you
     * @throws Exception any exceptions that should bubble up to Netty
     */
    protected abstract void encodeSpecificProperties(ByteBuf out) throws Exception;

    /**
     * Subclasses, when implementing this method, should have it return
     * effectively a clone of themselves, exception with newID instead of their
     * existing document ID.
     *
     * This is necessary to simplify the logic of assigning IDs to writes which are not
     * updates, upserts, or ID-specified index requests.
     *
     * @param newID the new {@link NabuWriteCommand#documentID}.
     * @return a copy of the current command, except with the documentID changed.
     */
    public abstract NabuWriteCommand copyWithNewId(String newID);
}
