package no.safebase.serde;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

public interface AppSerdes {

    <T extends SpecificRecord> SpecificAvroSerde<T> getKeySerde();

    <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerde();
}
