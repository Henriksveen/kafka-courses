package no.safebase.serde;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallAggregateValue;
import no.safebase.nobill.model.CallRecordKey;
import no.safebase.nobill.model.CallRecordValue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Windowed;

public interface NobillSerdes {

    SpecificAvroSerde<CallRecordKey> CallRecordKey();

    SpecificAvroSerde<CallRecordValue> CallRecordValue();

    SpecificAvroSerde<CallAggregateKey> CallAggregateKey();

    Serde<Windowed<CallAggregateKey>> WindowedCallAggregateKey(Long windowSize);

    SpecificAvroSerde<CallAggregateValue> CallAggregateValue();
}
