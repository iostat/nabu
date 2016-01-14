package io.stat.nabuproject.nabu.common.response;

/**
 * A Nabu FAIL response.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class FailResponse extends NabuResponse {
    public FailResponse(long sequence) {
        super(Type.FAIL, sequence);
    }
}
