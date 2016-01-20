package io.stat.nabuproject.nabu.elasticsearch;

import com.google.inject.Inject;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.telemetry.TelemetryCounterSink;
import io.stat.nabuproject.core.telemetry.TelemetryGaugeSink;
import io.stat.nabuproject.core.telemetry.TelemetryService;
import io.stat.nabuproject.nabu.common.command.IndexCommand;
import io.stat.nabuproject.nabu.common.command.NabuWriteCommand;
import io.stat.nabuproject.nabu.common.command.UpdateCommand;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import java.util.ArrayDeque;

/**
 * The canonical implementation of {@link NabuCommandESWriter}
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class CommandWriterImpl implements NabuCommandESWriter {
    private final ESClient esClient;
    private final TelemetryService telemetryService;
    private static final ESWriteResults GENERIC_FAIL = new ESWriteResults(false, -1);

    private final TelemetryCounterSink indexCommandsWritten;
    private final TelemetryCounterSink updateCommandsWritten;

    private final TelemetryCounterSink bulkIndexCount;
    private final TelemetryCounterSink bulkUpdateCount;

    private final TelemetryCounterSink bulkWrites;
    private final TelemetryCounterSink bulkWriteErrors;

    private final TelemetryGaugeSink indexNsGauge;
    private final TelemetryGaugeSink updateNsGauge;
    private final TelemetryGaugeSink bulkNsGauge;

    @Inject
    public CommandWriterImpl(ESClient esClient, TelemetryService telemetryService) {
        this.esClient = esClient;
        this.telemetryService = telemetryService;

        this.indexCommandsWritten = telemetryService.createCounter("commands.index.written");
        this.updateCommandsWritten = telemetryService.createCounter("commands.update.written");
        this.bulkWrites = telemetryService.createCounter("bulk.operations");
        this.bulkWriteErrors = telemetryService.createCounter("bulk.errors");

        this.bulkIndexCount = telemetryService.createCounter("bulk.index_count");
        this.bulkUpdateCount = telemetryService.createCounter("bulk.update_count");

        this.indexNsGauge = telemetryService.createGauge("exec.index.nanos");
        this.updateNsGauge = telemetryService.createGauge("exec.update.nanos");
        this.bulkNsGauge = telemetryService.createGauge("exec.bulk.nanos");
    }

    @Override
    public ESWriteResults singleWrite(NabuWriteCommand nwc) {
        Client realClient = esClient.getESClient();
        if(nwc instanceof IndexCommand) {
            IndexRequestBuilder irb = ic2irb(realClient, (IndexCommand) nwc);

            // todo: error checking. profiling.
            long start = System.nanoTime();
            IndexResponse done = irb.execute().actionGet();
            long end = System.nanoTime() - start;
            indexCommandsWritten.increment();
            indexNsGauge.set(end);
            return new ESWriteResults(done.isCreated(), end);
        } else if (nwc instanceof UpdateCommand) {
            UpdateRequestBuilder urb = uc2urb(realClient, (UpdateCommand) nwc);

            // todo: ditto
            long start = System.nanoTime();
            UpdateResponse done = urb.execute().actionGet();
            long end = System.nanoTime() - start;
            updateCommandsWritten.increment();
            updateNsGauge.set(end);
            return new ESWriteResults(done.isCreated(), end);
        }
        return GENERIC_FAIL;
    }

    @Override
    public ESWriteResults bulkWrite(ArrayDeque<NabuWriteCommand> commands) {
        Client realClient = esClient.getESClient();
        BulkRequestBuilder brb = realClient.prepareBulk();

        for(NabuWriteCommand c : commands) {
            if(c instanceof IndexCommand) {
                IndexRequestBuilder irb = ic2irb(realClient, (IndexCommand) c);
                brb.add(irb);
                bulkIndexCount.increment();
            } else if(c instanceof UpdateCommand) {
                UpdateRequestBuilder urb = uc2urb(realClient, (UpdateCommand) c);
                brb.add(urb);
                bulkUpdateCount.increment();
            }
        }

        BulkResponse response = brb.execute().actionGet();
        long reqTime = response.getTookInMillis();

        int failures = 0;

        for(BulkItemResponse bir : response.getItems()) {
            if(bir.isFailed()) {
                // todo: disabling for performance. stdout is SLOW
//                logger.warn("[ES BULK WRITE FAILURE] {}/{}/{}",
//                        bir.getIndex(),
//                        bir.getType(),
//                        bir.getId(),
//                        bir.getFailureMessage());
                failures++;
            }
        }

        long reqNanos = reqTime*1000;

        bulkNsGauge.set(reqNanos);
        bulkWrites.increment();
        bulkWriteErrors.delta(failures);

        // totally a good heuristic... right?
        if(failures > commands.size() / 2) {
            return new ESWriteResults(false, reqNanos);
        } else {
            return new ESWriteResults(true, reqNanos);
        }
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
