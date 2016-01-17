package io.stat.nabuproject.nabu.client;

import com.google.common.collect.ImmutableMap;
import io.stat.nabuproject.nabu.common.command.IndexCommand;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

/**
 * Used to build IndexCommands, and optionally executePreparedCommand them.
 * This class is not thread-safe.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode @ToString
public class UpdateCommandBuilder implements WriteCommandBuilder {
    final NabuClientConnectionState executor;

    String index;
    String documentType;
    String documentSource;
    String documentID;
    boolean shouldRefresh;
    String updateScript;
    boolean isUpsert;
    Map<String, Serializable> scriptParams;

    /**
     * Create an UpdateCommandBuilder WITHOUT an executor (set to null). This means
     * you will have to pass this UpdateCommandBuilder to the client and have it executePreparedCommand that.
     *
     * The document ID will be automatically generated when this command is executed.
     * The document source will be blank.
     * ES will not be instructed to trigger a refresh after the command executes.
     * The update script will be empty.
     * The update script params will be an empty map.
     * This update will not be treated as an upsert.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     */
    public UpdateCommandBuilder(String index, String documentType) {
        this(null, index, documentType);
    }

    /**
     * Create an UpdateCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * The document ID will be automatically generated when this command is executed.
     * The document source will be blank.
     * ES will not be instructed to trigger a refresh after the command executes.
     * The update script will be empty.
     * The update script params will be an empty map.
     * This update will not be treated as an upsert.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     */
    public UpdateCommandBuilder(NabuClientConnectionState executor, String index, String documentType) {
        this(executor, index, documentType,  "");
    }

    /**
     * Create an UpdateCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * The document source will be blank.
     * ES will not be instructed to trigger a refresh after the command executes.
     * The update script will be empty.
     * The update script params will be an empty map.
     * This update will not be treated as an upsert.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     * @param documentID the ID of the document.
     */
    public UpdateCommandBuilder(NabuClientConnectionState executor, String index, String documentType, String documentID) {
        this(executor, index, documentType, documentID, "");
    }

    /**
     * Create an UpdateCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * ES will not be instructed to trigger a refresh after the command executes.
     * The update script will be empty.
     * The update script params will be an empty map.
     * This update will not be treated as an upsert.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     * @param documentID the ID of the document.
     * @param documentSource the JSON source of the document
     */
    public UpdateCommandBuilder(NabuClientConnectionState executor, String index, String documentType, String documentID, String documentSource) {
        this(executor, index, documentType, documentID, documentSource, false);
    }

    /**
     * Create an UpdateCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * The update script will be empty.
     * The update script params will be an empty map.
     * This update will not be treated as an upsert.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     * @param documentID the ID of the document.
     * @param documentSource the JSON source of the document
     * @param shouldRefresh whether or not ES should trigger an index refresh after this command is finally executed.
     */
    public UpdateCommandBuilder(NabuClientConnectionState executor, String index, String documentType, String documentID, String documentSource, boolean shouldRefresh) {
        this(executor, index, documentType, documentID, documentSource, shouldRefresh, false);
    }

    /**
     * Create an UpdateCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * The update script will be empty.
     * The update script params will be an empty map.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     * @param documentID the ID of the document.
     * @param documentSource the JSON source of the document
     * @param shouldRefresh whether or not ES should trigger an index refresh after this command is finally executed.
     * @param isUpsert whether or not this update should be treated as an upsert
     */
    public UpdateCommandBuilder(NabuClientConnectionState executor, String index, String documentType, String documentID, String documentSource, boolean shouldRefresh, boolean isUpsert) {
        this(executor, index, documentType, documentID, documentSource, shouldRefresh, isUpsert, "");
    }

    /**
     * Create an UpdateCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * The update script params will be an empty map.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     * @param documentID the ID of the document.
     * @param documentSource the JSON source of the document
     * @param shouldRefresh whether or not ES should trigger an index refresh after this command is finally executed.
     * @param isUpsert whether or not this update should be treated as an upsert
     * @param updateScript the update script to run when executing this command
     */
    public UpdateCommandBuilder(NabuClientConnectionState executor, String index, String documentType,
                                String documentID, String documentSource, boolean shouldRefresh,
                                boolean isUpsert, String updateScript) {
        this(executor, index, documentType, documentID, documentSource, shouldRefresh, isUpsert, updateScript, ImmutableMap.of());
    }

    /**
     * Create an UpdateCommandBuilder with a specified executor. This means you will
     * be able to call {@link WriteCommandBuilder#execute()} directly. The client's
     * prepare* methods all create instances of a command builder an executor.
     *
     * @param index The ES index this command will operate on
     * @param documentType the document type (i.e. mapping) this command will operate on
     * @param documentID the ID of the document.
     * @param documentSource the JSON source of the document
     * @param shouldRefresh whether or not ES should trigger an index refresh after this command is finally executed.
     * @param isUpsert whether or not this update should be treated as an upsert
     * @param updateScript the update script to run when executing this command
     * @param scriptParams a map of parameters to pass to the script when it runs
     */
    public UpdateCommandBuilder(NabuClientConnectionState executor, String index, String documentType,
                                String documentID, String documentSource, boolean shouldRefresh,
                                boolean isUpsert, String updateScript, Map<String, Serializable> scriptParams) {
        this.executor = executor;
        this.index = index;
        this.documentType = documentType;
        this.documentID = documentID;
        this.documentSource = documentSource;
        this.shouldRefresh = shouldRefresh;
        this.isUpsert = isUpsert;
        this.updateScript = updateScript;
        this.scriptParams = scriptParams;
    }

    /**
     * Change the index to something else
     * @param idx the new index
     */
    public void onIndex(String idx) {
        index = idx;
    }

    /**
     * Change the type (mapping) of the document
     * @param newType the new type (mapping)
     */
    public void ofType(String newType) {
        documentType = newType;
    }

    /**
     * Change the document's JSON source
     * @param src the new document source
     */
    public void withSource(String src) {
        documentSource = src;
    }

    /**
     * Set the indexed document's ID
     * @param id the ID to write the document into
     */
    public void withID(String id) {
        documentID = id;
    }

    /**
     * Set whether or not the document will trigger a refresh when indexed
     * @param wellShouldIt whether or not an update op should be triggered after this command is executed
     */
    public void shouldRefresh(boolean wellShouldIt) {
        shouldRefresh = wellShouldIt;
    }

    /**
     * Whether or not the document should be created if it doesnt exist
     * @param wellIsIt well.. is it?
     */
    public void isUpsert(boolean wellIsIt) {
        isUpsert = wellIsIt;
    }

    /**
     * Change the update script associated with this document
     * @param theScript the document's new update script
     */
    public void withScript(String theScript) {
        updateScript = theScript;
    }

    /**
     * Sets the parameters that will be given to the update script
     * <b>AN IMMUTABLE COPY OF newParams WILL BE MADE. CHANGES MADE
     * TO ANY MAP YOU PASS IN WILL NOT BE INCLUDED AFTER THE SCRIPT
     * PARAMS ARE UPDATE.</b>
     * @param newParams the new parameters for the script
     */
    public void withScriptParams(Map<String, Serializable> newParams) {
        scriptParams = newParams;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IndexCommand build(long sequence) {
        return new IndexCommand(sequence, index, documentType, documentID, documentSource, shouldRefresh);
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
