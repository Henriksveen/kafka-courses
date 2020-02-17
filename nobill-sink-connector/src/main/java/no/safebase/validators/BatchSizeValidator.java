package no.safebase.validators;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class BatchSizeValidator implements ConfigDef.Validator {

    private static final int MAX_BATCH_SIZE = 50000;

    @Override
    public void ensureValid(String name, Object value) {
        Integer batchSize = (Integer) value;
        if (!(1 <= batchSize && batchSize <= MAX_BATCH_SIZE))
            throw new ConfigException(name, value, String.format("Batch Size mush be a positive integer that's less or equal to %s", MAX_BATCH_SIZE));
    }
}
