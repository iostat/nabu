package com.socialrank.nabu.common;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public abstract class NabuCommand {
    public static final short MAGIC = 0x4E43;

    @Getter final String index;
    @Getter final String documentType;
    final boolean shouldUpdate;

    public boolean shouldUpdateIndex() { return shouldUpdate; }

    public abstract NabuCommandType getType();

    public NabuCommand(String index, String documentType) {
        this(index, documentType, false);
    }

    public NabuCommand(String index, String documentType, boolean shouldUpdate) {
        this.index = index;
        this.documentType = documentType;
        this.shouldUpdate = shouldUpdate;
    }

    /**
     * Descendants encode any data that's specific to them (i.e., not defined up top)
     * into out.
     * @param out the ByteBuf to encode into.
     */
    public abstract void encodeSpecificsInto(ByteBuf out);
}
