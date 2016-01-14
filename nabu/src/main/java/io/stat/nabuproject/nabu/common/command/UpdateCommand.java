package io.stat.nabuproject.nabu.common.command;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.stat.nabuproject.core.net.ProtocolHelper;
import lombok.Getter;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Map;

/**
 * The NabuCommand parallel to ElasticSearch's Update API
 */
public class UpdateCommand extends NabuWriteCommand {
    @Getter final String updateScript;
    @Getter final ImmutableMap<String, Serializable> scriptParams;
    final boolean hasUpsert;
    final boolean hasUpdateScript;

    public boolean hasUpsert() {
        return hasUpsert;
    }
    public boolean hasUpdateScript() {
        return hasUpdateScript;
    }

    public UpdateCommand(long sequence,
                         @NonNull String index,
                         @NonNull String documentType,
                         @NonNull String documentID,
                         @NonNull String documentSource,
                         boolean shouldRefresh,
                         @NonNull String updateScript,
                         @NonNull Map<String, Serializable> scriptParams,
                         boolean hasUpsert,
                         boolean hasUpdateScript) {
        super(NabuCommand.Type.UPDATE, sequence, index, documentType, documentID, documentSource, shouldRefresh);
        this.updateScript = updateScript;
        this.scriptParams = ImmutableMap.copyOf(scriptParams);
        this.hasUpsert = hasUpsert;
        this.hasUpdateScript = hasUpdateScript;
    }

    @Override
    protected boolean hasEncodableProperties() {
        return true;
    }

    @Override
    protected void encodeSpecificProperties(ByteBuf out) throws Exception {
        out.writeBoolean(hasUpsert());
        out.writeBoolean(hasUpdateScript());
        ProtocolHelper.writeStringToByteBuf(getUpdateScript(), out);
        ProtocolHelper.writeSerializableToByteBuf(scriptParams, out);
    }

    @Override
    public UpdateCommand copyWithNewId(String newID) {
        return new UpdateCommand(getSequence(), getIndex(), getDocumentType(), newID, getDocumentSource(), shouldRefresh(),
                getUpdateScript(), getScriptParams(), hasUpsert(), hasUpdateScript());
    }
}
