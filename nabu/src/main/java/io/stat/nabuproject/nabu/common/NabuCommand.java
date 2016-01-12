package io.stat.nabuproject.nabu.common;

import lombok.Getter;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public abstract class NabuCommand {
    public static final short MAGIC = 0x4E43;
    public static final String MAGIC_HEX_STRING = "0x" + Integer.toHexString(MAGIC);

    @Getter final String index;
    @Getter final String documentType;
    final boolean shouldRefresh;

    public boolean shouldRefresh() {
        return shouldRefresh;
    }

    public abstract NabuCommandType getType();

    public NabuCommand(String index, String documentType, boolean shouldRefresh) {
        this.index = index;
        this.documentType = documentType;
        this.shouldRefresh = shouldRefresh;
    }

    public NabuCommand(NabuCommand base) {
        this.index = base.index;
        this.documentType = base.documentType;
        this.shouldRefresh = base.shouldRefresh;
    }
}
