package io.stat.nabuproject.nabu.common.response;

import lombok.Getter;

/**
 * A Nabu FAIL response.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class FailResponse extends NabuResponse {
    private final @Getter String id;
    private final @Getter String reason;

    public FailResponse(long sequence, String id,  String reason) {
        super(Type.FAIL, sequence);
        this.id = id;
        this.reason = reason;
    }
}
