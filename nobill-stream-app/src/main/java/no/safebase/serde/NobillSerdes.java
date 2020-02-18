package no.safebase.serde;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import no.safebase.nobill.model.*;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Windowed;

public interface NobillSerdes {

    <T extends SpecificRecord> SpecificAvroSerde<T> Key();

    <T extends SpecificRecord> SpecificAvroSerde<T> Value();

    Serde<Windowed<CallAggregateKey>> WindowedCallAggregateKey(Long windowSize);
}
