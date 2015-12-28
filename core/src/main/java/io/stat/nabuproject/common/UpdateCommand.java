package io.stat.nabuproject.common;

import lombok.Getter;
import lombok.NonNull;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class UpdateCommand extends NabuCommand {
    @Getter String documentSource;
    @Getter String updateScript;

    public UpdateCommand(NabuCommand base, @NonNull String documentSource, @NonNull String updateScript) {
        super(base);
        this.documentSource = documentSource;
        this.updateScript = updateScript;
    }

    @Override
    public NabuCommandType getType() {
        return NabuCommandType.UPDATE;
    }
}
