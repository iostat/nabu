package io.stat.nabuproject.nabu.common.response;

import lombok.Getter;

/**
 * A response by Nabu to an IDENTIFY request.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class IDResponse extends NabuResponse {
    private final @Getter String data;

    public IDResponse(long sequence, String data) {
        super(Type.ID, sequence);
        this.data = data;
    }

    @Override
    public String toString() {
        return String.format("<NabuResp_%s[%d]:%s>", getType().toString(), getSequence(), getData());
    }
}
