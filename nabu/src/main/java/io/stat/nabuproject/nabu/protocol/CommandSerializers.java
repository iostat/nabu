package io.stat.nabuproject.nabu.protocol;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.stat.nabuproject.core.util.ProtocolHelper;
import io.stat.nabuproject.nabu.common.IndexCommand;
import io.stat.nabuproject.nabu.common.NabuCommand;
import io.stat.nabuproject.nabu.common.NabuCommandType;
import io.stat.nabuproject.nabu.common.UpdateCommand;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Provides serialization and deserialization helpers for a variety of subclasses of
 * {@link NabuCommand}. Only used by members of {@link io.stat.nabuproject.nabu.protocol}, and not by you.
 */
@UtilityClass @Slf4j
class CommandSerializers {
    private interface NabuCommandSerializer<T extends NabuCommand> extends BiConsumer<T, ByteBuf> { }
    private interface NabuCommandDeserializer<T extends NabuCommand> extends BiFunction<ByteBuf, NabuCommand, T> { }

    private static final NabuCommandSerializer<IndexCommand> INDEX_COMMAND_SERIALIZER = (ic, out) ->
        ProtocolHelper.writeStringToByteBuf(ic.getDocumentSource(), out);

    private static final NabuCommandSerializer<UpdateCommand> UPDATE_COMMAND_SERIALIZER = (uc, out) -> {
        logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");
        logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");

        ProtocolHelper.writeStringToByteBuf(uc.getDocumentSource(), out);
        ProtocolHelper.writeStringToByteBuf(uc.getUpdateScript(), out);
    };

    private static final NabuCommandDeserializer<IndexCommand> INDEX_COMMAND_DESERIALIZER = (in, base) -> {
        String documentSource = ProtocolHelper.readStringFromByteBuf(in);

        return new IndexCommand(base, documentSource);
    };

    private static final NabuCommandDeserializer<UpdateCommand> UPDATE_COMMAND_DESERIALIZER = (in, base) -> {
        String documentSource = ProtocolHelper.readStringFromByteBuf(in);
        String updateScript   = ProtocolHelper.readStringFromByteBuf(in);

        logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");
        logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");
        logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");logger.error("UNIMPLEMENTED");
        return new UpdateCommand(base, documentSource, updateScript, "", ImmutableMap.of(), false, false);
    };

    public static void serialize(NabuCommand command, ByteBuf out) {
        if(command.getType() == NabuCommandType.INDEX) {
            INDEX_COMMAND_SERIALIZER.accept((IndexCommand)command, out);
        } else if(command.getType() == NabuCommandType.UPDATE) {
            UPDATE_COMMAND_SERIALIZER.accept((UpdateCommand)command, out);
        } else {
            throw new IllegalArgumentException("Cannot encode NabuCommands of type " + command.getType().toString());
        }
    }

    public static NabuCommand deserialize(ByteBuf in, NabuCommand base, NabuCommandType type) {
        if(type == NabuCommandType.INDEX) {
            return INDEX_COMMAND_DESERIALIZER.apply(in, base);
        } else if (type == NabuCommandType.UPDATE) {
            return UPDATE_COMMAND_DESERIALIZER.apply(in, base);
        } else {
            throw new IllegalArgumentException("Cannot decode NabuCommands of type "  + type.toString());
        }
    }
}
