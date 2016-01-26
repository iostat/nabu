package io.stat.nabuproject.nabu.common.command;

import io.netty.buffer.ByteBuf;
import io.stat.nabuproject.core.net.ProtocolHelper;
import lombok.Getter;
import lombok.NonNull;

/**
 * The NabuCommand parallel to ElasticSearch's Index API
 */
public class IndexCommand extends NabuWriteCommand {
    public static final String INDEX_OP_TYPE = "index";
    public static final String CREATE_OP_TYPE = "create";
    private final @Getter String opType;

    public IndexCommand(long sequence,
                        @NonNull String index,
                        @NonNull String docType,
                        @NonNull String docID,
                        @NonNull String documentSource,
                        boolean shouldRefresh,
                        boolean shouldForceWrite,
                        String opType) {
        super(Type.INDEX, sequence, index, docType, docID, documentSource, shouldRefresh, shouldForceWrite);
        this.opType = opType;
    }

    @Override
    protected boolean hasEncodableProperties() {
        return true;
    }

    @Override
    protected void encodeSpecificProperties(ByteBuf out) throws Exception {
        ProtocolHelper.writeStringToByteBuf(getOpType(), out);
    }

    @Override
    public IndexCommand copyWithNewId(String newID) {
        return new IndexCommand(getSequence(), getIndex(),
                getDocumentType(), newID, getDocumentSource(),
                shouldRefresh(), shouldForceWrite(), getOpType());
    }
}
