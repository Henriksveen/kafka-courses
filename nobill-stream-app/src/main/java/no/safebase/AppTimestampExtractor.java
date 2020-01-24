package no.safebase;

import no.safebase.nobill.model.CallRecordValue;
import no.safebase.utils.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class AppTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long partitionTime) {
        Object record = consumerRecord.value();
        if (record instanceof CallRecordValue)
            return DateUtils.toEpoch(((CallRecordValue) record).getStarttime());
        else
            return partitionTime;
    }
}
