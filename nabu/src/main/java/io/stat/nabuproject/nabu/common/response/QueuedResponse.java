package io.stat.nabuproject.nabu.common.response;

/**
 * A Nabu QUEUED response
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class QueuedResponse extends NabuResponse {
    public QueuedResponse(long sequence) {
        super(Type.QUEUED, sequence);
    }
}
