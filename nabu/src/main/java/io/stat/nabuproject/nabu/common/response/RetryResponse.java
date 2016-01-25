package io.stat.nabuproject.nabu.common.response;

import lombok.Getter;

/**
 * A Nabu RETRY response
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class RetryResponse extends NabuResponse {
    private final @Getter String id;
    public RetryResponse(long sequence, String id) {
        super(Type.RETRY, sequence);
        this.id = id;
    }
}
