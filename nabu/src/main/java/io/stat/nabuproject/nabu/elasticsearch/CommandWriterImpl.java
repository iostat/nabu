package io.stat.nabuproject.nabu.elasticsearch;

import com.google.inject.Inject;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.nabu.common.command.IndexCommand;
import io.stat.nabuproject.nabu.common.command.NabuWriteCommand;
import io.stat.nabuproject.nabu.common.command.UpdateCommand;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.util.List;

/**
 * The canonical implementation of {@link NabuCommandESWriter}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class CommandWriterImpl implements NabuCommandESWriter {
    private final ESClient esClient;

    @Inject
    public CommandWriterImpl(ESClient esClient) {
        this.esClient = esClient;
    }

    @Override
    public long singleWrite(NabuWriteCommand nwc) {
        Client realClient = esClient.getESClient();
        if(nwc instanceof IndexCommand) {
            IndexRequestBuilder irb = ic2irb(realClient, (IndexCommand) nwc);

            // todo: error checking. profiling.
            long start = System.currentTimeMillis();
            irb.execute().actionGet();
            return System.currentTimeMillis() - start;
        } else if (nwc instanceof UpdateCommand) {
            UpdateRequestBuilder urb = uc2urb(realClient, (UpdateCommand) nwc);

            // todo: ditto
            long start = System.currentTimeMillis();
            urb.execute().actionGet();
            return System.currentTimeMillis() - start;
        }
        return 0;
    }

    @Override
    public long bulkWrite(List<NabuWriteCommand> commands) {
        Client realClient = esClient.getESClient();
        BulkRequestBuilder brb = realClient.prepareBulk();

        for(NabuWriteCommand c : commands) {
            if(c instanceof IndexCommand) {
                IndexRequestBuilder irb = ic2irb(realClient, (IndexCommand) c);
                brb.add(irb);
            } else if(c instanceof UpdateCommand) {
                UpdateRequestBuilder urb = uc2urb(realClient, (UpdateCommand) c);
                brb.add(urb);
            }
        }

        BulkResponse response = brb.execute().actionGet();
        long reqTime = response.getTookInMillis();
        logger.info("Bulk request took {} millis, and not a single fuck was given that day.", reqTime);
        // todo: adjust batch size based on this somehow?

        for(BulkItemResponse bir : response.getItems()) {
            if(bir.isFailed()) {
                logger.warn("[ES BULK WRITE FAILURE] {}/{}[{}] :: {}",
                        bir.getIndex(),
                        bir.getType(),
                        bir.getId(),
                        bir.getFailureMessage());
            }
        }

        return reqTime;
    }

    private IndexRequestBuilder ic2irb(Client client, IndexCommand ic) {
        IndexRequestBuilder irb = client.prepareIndex();

        irb.setIndex(ic.getIndex())
                .setType(ic.getDocumentType())
                .setId(ic.getDocumentID())
                .setSource(ic.getDocumentSource())
                .setRefresh(ic.shouldRefresh());

        return irb;
    }

    private UpdateRequestBuilder uc2urb(Client client, UpdateCommand uc) {
        UpdateRequestBuilder urb = client.prepareUpdate();
        urb.setIndex(uc.getIndex())
                .setType(uc.getDocumentType())
                .setId(uc.getDocumentID())
                .setRefresh(uc.shouldRefresh())
                .setRetryOnConflict(5); // todo: configurable in ThrottlePolicy? or nah?

        if(uc.hasUpdateScript()) {
            urb.setScript(new Script(
                    uc.getUpdateScript(),
                    ScriptService.ScriptType.INLINE,
                    null,
                    uc.getScriptParams()
            ));

            if(uc.hasUpsert()) {
                urb.setUpsert(uc.getDocumentSource());
            }
        } else {
            urb.setDoc(uc.getDocumentSource());
            if(uc.hasUpsert()) {
                urb.setDocAsUpsert(true);
            }
        }

        return urb;
    }
}
