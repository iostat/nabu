package io.stat.nabuproject.core.throttling;

import io.stat.nabuproject.core.config.ConfigMapper;
import io.stat.nabuproject.core.config.InvalidValueException;
import io.stat.nabuproject.core.config.MappedConfigObject;
import io.stat.nabuproject.core.config.MissingPropertyException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a throttle policy. These are ideally passed on
 * as {@link java.util.concurrent.atomic.AtomicReference}s,
 * as since the policies are immutable, the provider simply needs to
 * change the reference and all children will see the updated values.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode @ToString
public class ThrottlePolicy implements MappedConfigObject<ThrottlePolicy> {
    private static final long serialVersionUID = -3488467207469822148L;

    /**
     * The default prefix to add when mapping an index name to map an ES index to
     * an Kafka topic
     */
    public static final String TOPIC_PREFIX = "nabu-";

    /**
     * Default value if <tt>target_time</tt> property isn't set.
     */
    public static final long DEFAULT_WRITE_TIME_GOAL = 500;

    /**
     * Default maximum batch size, for when <tt>max_batch</tt> is unspecified
     */
    public static final int DEFAULT_MAX_BATCH_SIZE = 500;

    /**
     * Default flush timeout, in millseconds, when <tt>flush_timeout</tt> isn't set.
     */
    public static final int DEFAULT_FLUSH_TIMEOUT = 2000;

    /**
     * The name of the ES index this policy describes
     */
    private final @Getter String indexName;

    /**
     * The corresponding Kafka topic for this policy's ES index
     */
    private final @Getter String topicName;

    /**
     * The write rate to attempt to maintain when scaling batch and flush sizes (in milliseconds).
     */
    private final @Getter long writeTimeTarget;

    /**
     * The maximum batch size (in documents).
     */
    private final @Getter int maxBatchSize;

    /**
     * If a batch isn't filled all the way, how long to wait (in milliseconds)
     * before attempting to flush it.
     */
    private final @Getter long flushTimeout;

    public ThrottlePolicy(String indexName) { this(indexName, DEFAULT_WRITE_TIME_GOAL); }
    public ThrottlePolicy(String indexName, long writeTimeGoal) { this(indexName, writeTimeGoal, DEFAULT_MAX_BATCH_SIZE); }
    public ThrottlePolicy(String indexName, long writeTimeGoal, int maxBatchSize) { this(indexName, writeTimeGoal, maxBatchSize, DEFAULT_FLUSH_TIMEOUT); }
    public ThrottlePolicy(String indexName, long writeTimeGoal, int maxBatchSize, long flushTimeout) { this(indexName, TOPIC_PREFIX, writeTimeGoal, maxBatchSize, flushTimeout); }
    public ThrottlePolicy(String indexName, String topicName) { this(indexName, topicName, DEFAULT_WRITE_TIME_GOAL); }
    public ThrottlePolicy(String indexName, String topicName, long writeTimeGoal) { this(indexName, topicName, writeTimeGoal, DEFAULT_MAX_BATCH_SIZE); }
    public ThrottlePolicy(String indexName, String topicName, long writeTimeGoal, int maxBatchSize) { this(indexName, topicName, writeTimeGoal, maxBatchSize, DEFAULT_FLUSH_TIMEOUT); }
    public ThrottlePolicy(String indexName, String topicName, long writeTimeGoal, int maxBatchSize, long flushTimeout) {
        this.indexName = indexName;
        this.topicName = topicName;
        this.writeTimeTarget = writeTimeGoal;
        this.maxBatchSize = maxBatchSize;
        this.flushTimeout = flushTimeout;
    }

    @Override
    public ConfigMapper<ThrottlePolicy> getMapper() {
        return _configMapper;
    }

    private static final ConfigMapper<ThrottlePolicy> _configMapper = (map -> {
        Logger logger = LoggerFactory.getLogger("ConfigMapper$ThrottlePolicy");
        if(!map.containsKey("index")) {
            throw new MissingPropertyException("Throttle policy does not have an ElasticSearch index associated with it!");
        }

        Object maybeIndex = map.get("index");
        if(!(maybeIndex instanceof String)) {
            throw new InvalidValueException("Throttle policy ElasticSearch index name must be a String!");
        }

        String theIndex = maybeIndex.toString();

        Object maybeTopic = map.get("topic");
        if(!(maybeTopic instanceof String)) {
            throw new InvalidValueException("Throttle policy Kafka topic must be a String!");
        }

        String theTopic = maybeTopic.toString();

        long writeTimeGoal;
        int maxBatchSize;
        long flushTimeout;

        try {
            writeTimeGoal = ((Number)map.getOrDefault("target_time", DEFAULT_WRITE_TIME_GOAL)).longValue();
        } catch(ClassCastException cce) {
            writeTimeGoal = DEFAULT_WRITE_TIME_GOAL;
            logger.warn("Could not read the target write time for index policy" + theIndex + ", defaulting to " + writeTimeGoal, cce);
        }

        try {
            maxBatchSize = ((Number)map.getOrDefault("max_batch", DEFAULT_MAX_BATCH_SIZE)).intValue();
        } catch(ClassCastException cce) {
            maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
            logger.warn("Could not read the max batch size for index policy" + theIndex + ", defaulting to " + writeTimeGoal, cce);
        }

        try {
            flushTimeout = ((Number)map.getOrDefault("flush_timeout", DEFAULT_FLUSH_TIMEOUT)).longValue();
        } catch(ClassCastException cce) {
            flushTimeout = DEFAULT_MAX_BATCH_SIZE;
            logger.warn("Could not read the flush timeout for index policy" + theIndex + ", defaulting to " + writeTimeGoal, cce);
        }

        return new ThrottlePolicy(theIndex, theTopic, writeTimeGoal, maxBatchSize, flushTimeout);
    });
}
