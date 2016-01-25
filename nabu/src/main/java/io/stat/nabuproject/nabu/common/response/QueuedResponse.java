package io.stat.nabuproject.nabu.common.response;

import lombok.Getter;

/**
 * A Nabu QUEUED response
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
public class QueuedResponse extends NabuResponse {
    private final @Getter String id;
    public QueuedResponse(long sequence, String id) {
        super(Type.QUEUED, sequence);
        this.id = id;
    }
}
