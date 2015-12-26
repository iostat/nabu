package io.stat.nabu.protocol;

import io.netty.buffer.ByteBuf;
import io.stat.nabu.common.IndexCommand;
import io.stat.nabu.common.NabuCommand;
import io.stat.nabu.common.NabuCommandType;
import io.stat.nabu.common.UpdateCommand;
import lombok.experimental.UtilityClass;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * Provides serialization and deserialization helpers for a variety of subclasses of
 * {@link NabuCommand}. Only used by members of {@link io.stat.nabu.protocol}, and not by you.
 */
@UtilityClass
class CommandSerializers {
    private interface NabuCommandSerializer<T extends NabuCommand> extends BiConsumer<T, ByteBuf> { }
    private interface NabuCommandDeserializer<T extends NabuCommand> extends BiFunction<ByteBuf, NabuCommand, T> { }

    private static final NabuCommandSerializer<IndexCommand> INDEX_COMMAND_SERIALIZER = (ic, out) ->
        ProtocolHelpers.writeStringToByteBuf(ic.getDocumentSource(), out);

    private static final NabuCommandSerializer<UpdateCommand> UPDATE_COMMAND_SERIALIZER = (uc, out) -> {
        ProtocolHelpers.writeStringToByteBuf(uc.getDocumentSource(), out);
        ProtocolHelpers.writeStringToByteBuf(uc.getUpdateScript(), out);
    };

    private static final NabuCommandDeserializer<IndexCommand> INDEX_COMMAND_DESERIALIZER = (in, base) -> {
        String documentSource = ProtocolHelpers.readStringFromByteBuf(in);

        return new IndexCommand(base, documentSource);
    };

    private static final NabuCommandDeserializer<UpdateCommand> UPDATE_COMMAND_DESERIALIZER = (in, base) -> {
        String documentSource = ProtocolHelpers.readStringFromByteBuf(in);
        String updateScript   = ProtocolHelpers.readStringFromByteBuf(in);

        return new UpdateCommand(base, documentSource, updateScript);
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
