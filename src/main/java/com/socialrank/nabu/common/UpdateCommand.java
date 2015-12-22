package com.socialrank.nabu.common;

import com.socialrank.nabu.protocol.ProtocolHelpers;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
public class UpdateCommand extends NabuCommand {
    @Getter String documentSource;
    @Getter String updateScript;

    public UpdateCommand(String index, String documentType) {
        super(index, documentType);
    }

    public UpdateCommand(String index, String documentType, boolean shouldUpdate) {
        super(index, documentType, shouldUpdate);
    }

    @Override
    public NabuCommandType getType() {
        return NabuCommandType.UPDATE;
    }

    @Override
    public void encodeSpecificsInto(ByteBuf out) {
        ProtocolHelpers.writeStringToByteBuf(getDocumentSource(), out);
        ProtocolHelpers.writeStringToByteBuf(getUpdateScript(), out);
    }
}
