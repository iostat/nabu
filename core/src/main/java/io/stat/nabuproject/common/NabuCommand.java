package io.stat.nabuproject.common;

import lombok.Getter;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public abstract class NabuCommand {
    public static final short MAGIC = 0x4E43;
    public static final String MAGIC_HEX_STRING = "0x" + Integer.toHexString(MAGIC);

    @Getter final String index;
    @Getter final String documentType;
    final boolean shouldUpdate;

    public boolean shouldUpdateIndex() { return shouldUpdate; }

    public abstract NabuCommandType getType();

    public NabuCommand(String index, String documentType, boolean shouldUpdate) {
        this.index = index;
        this.documentType = documentType;
        this.shouldUpdate = shouldUpdate;
    }

    public NabuCommand(NabuCommand base) {
        this.index = base.index;
        this.documentType = base.documentType;
        this.shouldUpdate = base.shouldUpdate;
    }
}
