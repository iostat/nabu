package io.stat.nabuproject.nabu.router;

import com.google.inject.Inject;
import io.stat.nabuproject.core.elasticsearch.ESClient;
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
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor(onConstructor=@__(@Inject))
class RouterImpl extends CommandRouter {
    private final NabuKafkaProducer kafkaProducer;
    private final ThrottlePolicyProvider throttlePolicyProvider;
    private final NabuCommandESWriter esWriter;
    private final ESClient esClient;
    private final ESTimeBasedUUIDGen uuidGen;

    @Override
    public void inboundCommand(NabuCommandSource src, NabuCommand command) {
        if(command instanceof IndexCommand) {
            routeIndex(src, (IndexCommand) command);
        } else if(command instanceof UpdateCommand) {
            routeUpdate(src, (UpdateCommand) command);
        } else {
            src.respond(command.failResponse());
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
        ThrottlePolicy maybeTP = throttlePolicyProvider.getTPForIndex(command.getIndex());

        if(maybeTP == null) {
            try {
                ESWriteResults res = esWriter.singleWrite(command);
                src.respond(res.success() ? command.okResponse() : command.failResponse());
            } catch(Exception e) {
                logger.error("ElasticSearch spit out an error", e);
                src.respond(command.retryResponse());
            }
        } else {
            kafkaProducer.enqueueCommand(ThrottlePolicy.TOPIC_PREFIX + command.getIndex(), calculatePartition(command), src, command);
        }
    }

    private int calculatePartition(NabuWriteCommand command) {
        // todo: support commands with custom routing
        // todo: in order to get a perfect 1:1 mapping of ES's own write distribution
        int partitionsForIndex = esClient.getShardCount(command.getIndex());
        int hash = mm3.hash(command.getDocumentID());

        return MathUtils.mod(hash, partitionsForIndex);
    }

    private static final Murmur3HashFunction mm3 = new Murmur3HashFunction();
}
