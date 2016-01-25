package io.stat.nabuproject.nabu.common.command;

import io.netty.buffer.ByteBuf;
import lombok.NonNull;

/**
 * The NabuCommand parallel to ElasticSearch's Index API
 */
public class IndexCommand extends NabuWriteCommand {
    public IndexCommand(long sequence,
                        @NonNull String index,
                        @NonNull String docType,
                        @NonNull String docID,
                        @NonNull String documentSource,
                        boolean shouldRefresh,
                        boolean shouldForceWrite) {
        super(Type.INDEX, sequence, index, docType, docID, documentSource, shouldRefresh, shouldForceWrite);
    }

    @Override
    protected boolean hasEncodableProperties() {
        return false;
    }

    @Override
    protected void encodeSpecificProperties(ByteBuf out) throws Exception {
        /* no-op */
    }

    @Override
    public IndexCommand copyWithNewId(String newID) {
        return new IndexCommand(getSequence(), getIndex(),
                getDocumentType(), newID, getDocumentSource(),
                shouldRefresh(), shouldForceWrite());
    }
}
