package io.stat.nabuproject.nabu.client;

import io.stat.nabuproject.nabu.common.response.NabuResponse;

/**
 * Used internally to bridge the communication of the low-level {@link NabuClientIO} and
 * the high-level {@link NabuClient}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
interface HighLevelNabuClientBridge {
    /**
     * Called when a NabuResponse packet is received.
     * @param src the NabuClientIO which received it
     * @param response the packet that was received
     */
    void responseReceived(NabuClientIO src, NabuResponse response);

    /**
     * Called when the connection has been successfully established
     * (communication is established and an IDResponse was received that
     * matches the expected cluster name)
     * @param src the NabuClientIO which is in a ready state.
     */
    void connectionEstablished(NabuClientIO src);

    /**
     * Called when the connection to the Nabu server has been lost
     * @param src the NabuClientIO which has disconnected.
     */
    void connectionLost(NabuClientIO src);

    /**
     * Called when a connection was established, but the identification request
     * failed or timed out.
     * @param src the NabuClientIO which received the incorrect IDResponse
     * @param expectedClusterName the ES cluster name it was expecting
     * @param remoteClusterName the ES cluster name it received
     */
    void identificationFailed(NabuClientIO src, String expectedClusterName, String remoteClusterName);

    /**
     * Used internally when the connection is ready to set the low-level
     * IO object, making the bridge bidirectional.
     * @param ncio the low-level IO
     */
    void setClientIO(NabuClientIO ncio);
}
