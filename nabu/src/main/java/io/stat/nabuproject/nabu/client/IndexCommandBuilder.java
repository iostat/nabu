package io.stat.nabuproject.nabu.client;

import io.stat.nabuproject.nabu.common.command.IndexCommand;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Used to build IndexCommands, and optionally executePreparedCommand them.
 * This class is not thread-safe.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode @ToString
public class IndexCommandBuilder implements WriteCommandBuilder {
    final NabuClient executor;

    String index;
    String documentType;
    String documentSource;
    String documentID;
    boolean shouldRefresh;
    boolean shouldForceWrite;

    /**
     * Create an IndexCommandBuilder WITHOUT an executor (set to null). This means
     * you will have to pass this IndexCommandBuilder to the client and have it executePreparedCommand that.
     *
     * The document ID will be automatically generated when this command is executed.
     * The document source will be blank.
     * ES will not be instructed to trigger a refresh after the command executes.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     */
    public IndexCommandBuilder(String index, String documentType) {
        this(null, index, documentType);
    }

    /**
     * Create an IndexCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * The document ID will be automatically generated when this command is executed.
     * The document source will be blank.
     * ES will not be instructed to trigger a refresh after the command executes.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     */
    public IndexCommandBuilder(NabuClient executor, String index, String documentType) {
        this(executor, index, documentType,  "");
    }

    /**
     * Create an IndexCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * The document source will be blank.
     * ES will not be instructed to trigger a refresh after the command executes.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     * @param documentID the ID of the document.
     */
    public IndexCommandBuilder(NabuClient executor, String index, String documentType, String documentID) {
        this(executor, index, documentType, documentID, "");
    }

    /**
     * Create an IndexCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * ES will not be instructed to trigger a refresh after the command executes.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     * @param documentID the ID of the document.
     * @param documentSource the JSON source of the document
     */
    public IndexCommandBuilder(NabuClient executor, String index, String documentType, String documentID, String documentSource) {
        this(executor, index, documentType, documentID, documentSource, false);
    }

    /**
     * Create an IndexCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     * @param documentID the ID of the document.
     * @param documentSource the JSON source of the document
     * @param shouldRefresh whether or not ES should trigger an index refresh after this command is finally executed.
     */
    public IndexCommandBuilder(NabuClient executor, String index, String documentType, String documentID, String documentSource, boolean shouldRefresh) {
        this.executor = executor;
        this.index = index;
        this.documentType = documentType;
        this.documentID = documentID;
        this.documentSource = documentSource;
        this.shouldRefresh = shouldRefresh;
    }

    /**
     * Change the index to something else
     * @param idx the new index
     */
    public IndexCommandBuilder onIndex(String idx) {
        index = idx;
        return this;
    }

    /**
     * Change the type (i.e., mapping) to something else
     * @param newType the new mapping.
     */
    public IndexCommandBuilder ofType(String newType) {
        documentType = newType;
        return this;
    }

    /**
     * Set the indexed document's ID
     * @param id the ID to write the document into
     */
    public IndexCommandBuilder withID(String id) {
        documentID = id;
        return this;
    }

    /**
     * Change the document's JSON source
     * @param src the new document source
     */
    public IndexCommandBuilder withSource(String src) {
        documentSource = src;
        return this;
    }

    /**
     * Set whether or not the document will trigger a refresh when indexed
     * @param wellShouldIt whether or not an update op should be triggered after this command is executed
     */
    public IndexCommandBuilder shouldRefresh(boolean wellShouldIt) {
        shouldRefresh = wellShouldIt;
        return this;
    }

    /**
     * Set whether or not the router should ignore existing throttle policies
     * when routing this command.
     * @param wellShouldIt whether or not throttle policies are ignored for this command.
     */
    public IndexCommandBuilder shouldForceWrite(boolean wellShouldIt) {
        shouldForceWrite = wellShouldIt;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexCommand build(long sequence) {
        return new IndexCommand(sequence, index, documentType, documentID, documentSource, shouldRefresh, shouldForceWrite);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NabuClientFuture execute() throws NabuClientDisconnectedException {
        if(executor == null) {
            throw new IllegalStateException("Tried to executePreparedCommand() an index command that did not have an executor specified!");
        }

        return executor.executePreparedCommand(this);
    }
}
