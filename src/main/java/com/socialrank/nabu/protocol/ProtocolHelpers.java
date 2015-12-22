package com.socialrank.nabu.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.DecoderException;
import lombok.experimental.UtilityClass;

import java.nio.CharBuffer;
import java.nio.charset.Charset;

/**
 * Created by io on 12/22/15. (929) 253-6977 $50/hr
 */
@UtilityClass
public class ProtocolHelpers {
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    public static void writeStringToByteBuf(String s, ByteBuf out) {
        ByteBuf encodedString = ByteBufUtil.encodeString(UnpooledByteBufAllocator.DEFAULT, CharBuffer.wrap(s), UTF_8);
        int size = encodedString.readableBytes();

        out.writeInt(size);
        out.writeBytes(encodedString);
    }

    public static String readStringFromByteBuf(ByteBuf in) throws DecoderException {
        if(in.readableBytes() < 4) {
            throw new DecoderException("Don't have enough data available to determine a String length");
        }

        int size = in.readInt();

        if(in.readableBytes() < size) {
            throw new DecoderException("Don't have enough data to read the whole string");
        }

        ByteBuf stringBuf = Unpooled.buffer();
        in.readBytes(stringBuf, size);

        return stringBuf.toString(UTF_8);
    }
}
