package io.stat.nabuproject.nabu.client;

import lombok.Getter;

/**
 * Represents *the* state machine state of {@link NabuClientConnectionState}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
enum NCCState {
    INITIALIZED(0),
    CONNECTING(10),
    IDENTIFYING(20),
    SHUTTING_DOWN(40),
    DISCONNECTED(50),
    RUNNING(500),
    FAILED(999);

    private final @Getter int stateNum;

    NCCState(int stateNum) {
        this.stateNum = stateNum;
    }
}
