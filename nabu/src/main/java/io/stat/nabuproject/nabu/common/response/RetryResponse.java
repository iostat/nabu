package io.stat.nabuproject.nabu.common.response;

/**
 * A Nabu RETRY response
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class RetryResponse extends NabuResponse {
    public RetryResponse(long sequence) {
        super(Type.RETRY, sequence);
    }
}
