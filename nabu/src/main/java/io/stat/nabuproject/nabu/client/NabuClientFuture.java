package io.stat.nabuproject.nabu.client;

import io.stat.nabuproject.nabu.common.response.NabuResponse;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.concurrent.CompletableFuture;

/**
 * An extension of {@link CompletableFuture} that attaches
 * the expected sequence number of the response.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@RequiredArgsConstructor(access=AccessLevel.PACKAGE)
@EqualsAndHashCode(callSuper=true)
@ToString
public class NabuClientFuture extends CompletableFuture<NabuResponse> {
    private final @Getter long sequence;
}
