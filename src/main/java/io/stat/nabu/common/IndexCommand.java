package io.stat.nabu.common;

import lombok.Getter;
import lombok.NonNull;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class IndexCommand extends NabuCommand {
    @Getter String documentSource;

    public IndexCommand(NabuCommand base, @NonNull String documentSource) {
        super(base);
        this.documentSource = documentSource;
    }


    @Override
    public NabuCommandType getType() {
        return NabuCommandType.INDEX;
    }
}
