package io.stat.nabuproject.nabu.common.response;

/**
 * A Nabu OK response.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class OKResponse extends NabuResponse {
    public OKResponse(long sequence) {
        super(Type.OK, sequence);
    }
}
