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
 * Represents a throttling policy.
 *
 * @author Ilya Ostrovskiy (https://github.com/iostat/)
 */
@EqualsAndHashCode @ToString
public class ThrottlePolicy implements MappedConfigObject<ThrottlePolicy> {
    private static final long serialVersionUID = -3488467207469822148L;

    public static final long DEFAULT_WRITE_TIME_TARGET = 500;
    public static final int DEFAULT_MAX_BATCH_SIZE = 500;

    private final @Getter String indexName;
    private final @Getter long writeTimeTarget;
    private final @Getter int maxBatchSize;

    public ThrottlePolicy(String indexName) {
        this(indexName, DEFAULT_WRITE_TIME_TARGET, DEFAULT_MAX_BATCH_SIZE);
    }

    public ThrottlePolicy(String indexName, long writeTimeGoal){
        this(indexName, writeTimeGoal, DEFAULT_MAX_BATCH_SIZE);
    }

    public ThrottlePolicy(String indexName, long writeTimeGoal, int maxBatchSize) {
        this.indexName = indexName;
        this.writeTimeTarget = writeTimeGoal;
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public ConfigMapper<ThrottlePolicy> getMapper() {
        return _configMapper;
    }

    private static final ConfigMapper<ThrottlePolicy> _configMapper = (map -> {
        Logger logger = LoggerFactory.getLogger("ConfigMapper$ThrottlePolicy");
        if(!map.containsKey("index")) {
            throw new MissingPropertyException("Throttle policy does not have an index associated with it!");
        }

        Object maybeIndex = map.get("index");
        if(!(maybeIndex instanceof String)) {
            throw new InvalidValueException("Throttle policy index must be a String!");
        }

        String theIndex = maybeIndex.toString();

        long writeTimeGoal;
        int maxBatchSize;

        try {
            writeTimeGoal = ((Number)map.getOrDefault("target", DEFAULT_WRITE_TIME_TARGET)).longValue();
        } catch(ClassCastException cce) {
            writeTimeGoal = DEFAULT_WRITE_TIME_TARGET;
            logger.warn("Could not read the target write time for index policy" + theIndex + ", defaulting to " + writeTimeGoal, cce);
        }

        try {
            maxBatchSize = ((Number)map.getOrDefault("target", DEFAULT_MAX_BATCH_SIZE)).intValue();
        } catch(ClassCastException cce) {
            maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
            logger.warn("Could not read the max batch size for index policy" + theIndex + ", defaulting to " + writeTimeGoal, cce);
        }

        return new ThrottlePolicy(theIndex, writeTimeGoal, maxBatchSize);
    });
}
