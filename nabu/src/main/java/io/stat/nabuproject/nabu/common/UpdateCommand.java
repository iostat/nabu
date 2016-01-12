package io.stat.nabuproject.nabu.common;

import lombok.Getter;
import lombok.NonNull;

import java.util.Map;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class UpdateCommand extends NabuCommand {
    @Getter final String documentSource;
    @Getter final String updateScript;
    @Getter final String docID;
    @Getter final Map<String, Object> scriptParams;
    final boolean hasUpsert;
    final boolean hasUpdateScript;

    public boolean hasUpsert() {
        return hasUpsert;
    }

    public boolean hasUpdateScript() {
        return hasUpdateScript;
    }

    public UpdateCommand(NabuCommand base,
                         @NonNull String documentSource,
                         @NonNull String updateScript,
                         @NonNull String docID,
                         @NonNull Map<String, Object> scriptParams,
                         @NonNull boolean hasUpsert,
                         @NonNull boolean hasUpdateScript) {
        super(base);
        this.documentSource = documentSource;
        this.updateScript = updateScript;
        this.docID = docID;
        this.scriptParams = scriptParams;
        this.hasUpsert = hasUpsert;
        this.hasUpdateScript = hasUpdateScript;
    }

    @Override
    public NabuCommandType getType() {
        return NabuCommandType.UPDATE;
    }
}
