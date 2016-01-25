package io.stat.nabuproject.nabu.router;

import com.google.inject.Inject;
import io.stat.nabuproject.core.elasticsearch.ESClient;
import io.stat.nabuproject.core.telemetry.TelemetryCounterSink;
import io.stat.nabuproject.core.telemetry.TelemetryService;
import io.stat.nabuproject.core.throttling.ThrottlePolicy;
import io.stat.nabuproject.core.throttling.ThrottlePolicyProvider;
import io.stat.nabuproject.nabu.common.command.IndexCommand;
import io.stat.nabuproject.nabu.common.command.NabuCommand;
import io.stat.nabuproject.nabu.common.command.NabuWriteCommand;
import io.stat.nabuproject.nabu.common.command.UpdateCommand;
import io.stat.nabuproject.nabu.elasticsearch.ESWriteResults;
import io.stat.nabuproject.nabu.elasticsearch.NabuCommandESWriter;
import io.stat.nabuproject.nabu.kafka.NabuKafkaProducer;
import io.stat.nabuproject.nabu.server.NabuCommandSource;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.ESTimeBasedUUIDGen;
import org.elasticsearch.common.math.MathUtils;

/**
 * The internal command router.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@Slf4j
class RouterImpl extends CommandRouter {
    private final NabuKafkaProducer kafkaProducer;
    private final ThrottlePolicyProvider throttlePolicyProvider;
    private final NabuCommandESWriter esWriter;
    private final ESClient esClient;
    private final ESTimeBasedUUIDGen uuidGen;
    private final TelemetryCounterSink inboundIndexes;
    private final TelemetryCounterSink inboundUpdates;
    private final TelemetryCounterSink routedDirects;
    private final TelemetryCounterSink routedQueues;
    private final TelemetryCounterSink routingFails;
    private final TelemetryCounterSink shardCalcs;

    @Inject
    @java.beans.ConstructorProperties({"kafkaProducer", "throttlePolicyProvider", "esWriter", "esClient", "uuidGen"})
    public RouterImpl(NabuKafkaProducer kafkaProducer,
                      ThrottlePolicyProvider throttlePolicyProvider,
                      NabuCommandESWriter esWriter,
                      ESClient esClient,
                      ESTimeBasedUUIDGen uuidGen,
                      TelemetryService telemetryService) {
        this.kafkaProducer = kafkaProducer;
        this.throttlePolicyProvider = throttlePolicyProvider;
        this.esWriter = esWriter;
        this.esClient = esClient;
        this.uuidGen = uuidGen;

        this.inboundIndexes = telemetryService.createCounter("inbound", "type:index");
        this.inboundUpdates = telemetryService.createCounter("inbound", "type:update");
        this.routedDirects  = telemetryService.createCounter("response", "type:writethrough");
        this.routedQueues   = telemetryService.createCounter("response", "type:queued");
        this.routingFails   = telemetryService.createCounter("response", "type:fail");
        this.shardCalcs      = telemetryService.createCounter("shard.calc");
    }

    @Override
    public void inboundCommand(NabuCommandSource src, NabuCommand command) {
        if(command instanceof IndexCommand) {
            inboundIndexes.increment();
            routeIndex(src, (IndexCommand) command);
        } else if(command instanceof UpdateCommand) {
            inboundUpdates.increment();
            routeUpdate(src, (UpdateCommand) command);
        } else {
            routingFails.increment();
            src.respond(command.failResponse("", String.format("[CommandRouter] Don't know how to route command of type %s", command.getType())));
        }
    }

    private void routeIndex(NabuCommandSource src, IndexCommand command) {
        if(command.getDocumentID().isEmpty()) {
            routeIndex(src, command.copyWithNewId(uuidGen.getBase64UUID()));
        } else {
            routeCommand(src, command);
        }
    }

    private void routeUpdate(NabuCommandSource src, UpdateCommand command) {
        if(command.getDocumentID().isEmpty()) {
            // todo: is this even legal? (update request with no document ID)
            routeUpdate(src, command.copyWithNewId(uuidGen.getBase64UUID()));
        } else {
            routeCommand(src, command);
        }
    }

    private void routeCommand(NabuCommandSource src, NabuWriteCommand command) {
        ThrottlePolicy maybeTP = throttlePolicyProvider.getTPForIndex(command.getIndex()).get();

        if(maybeTP == null) {
            routedDirects.increment();
            try {
                ESWriteResults res = esWriter.singleWrite(command);
                src.respond(res.success() ? command.okResponse(command.getDocumentID()) : command.failResponse(command.getDocumentID(), res.getMessage()));
            } catch(Exception e) {
                logger.error("ElasticSearch spit out an error", e);
                src.respond(command.retryResponse(command.getDocumentID()));
            }
        } else {
            routedQueues.increment();
            kafkaProducer.enqueueCommand(ThrottlePolicy.TOPIC_PREFIX + command.getIndex(), calculatePartition(command), src, command);
        }
    }

    private int calculatePartition(NabuWriteCommand command) {
        // todo: support commands with custom routing
        // todo: in order to get a perfect 1:1 mapping of ES's own write distribution
        shardCalcs.increment();
        int partitionsForIndex = esClient.getShardCount(command.getIndex());
        int hash = mm3.hash(command.getDocumentID());

        return MathUtils.mod(hash, partitionsForIndex);
    }

    private static final Murmur3HashFunction mm3 = new Murmur3HashFunction();
}
