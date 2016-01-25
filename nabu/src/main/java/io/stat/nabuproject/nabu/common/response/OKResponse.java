package io.stat.nabuproject.nabu.common.response;

import lombok.Getter;

/**
 * A Nabu OK response.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class OKResponse extends NabuResponse {
    private @Getter String id;
    public OKResponse(long sequence, String id) {
        super(Type.OK, sequence);
        this.id = id;
    }
}
