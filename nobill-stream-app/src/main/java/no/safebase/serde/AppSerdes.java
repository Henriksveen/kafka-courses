package no.safebase.serde;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import no.safebase.AppConfig;
import no.safebase.nobill.model.*;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.util.HashMap;
import java.util.Map;

public class AppSerdes implements NobillSerdes {

    @Override
    public <T extends SpecificRecord> SpecificAvroSerde<T> Key() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, true);
        return serde;
    }

    @Override
    public <T extends SpecificRecord> SpecificAvroSerde<T> Value() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, false);
        return serde;
    }

/*    @Override
    public SpecificAvroSerde<NobillRecordKey> NobillRecordKey() {
        SpecificAvroSerde<NobillRecordKey> serde = new SpecificAvroSerde<>();
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, true);
        return serde;
    }

    @Override
    public SpecificAvroSerde<CallRecordValue> CallRecordValue() {
        SpecificAvroSerde<CallRecordValue> serde = new SpecificAvroSerde<>();
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, false);
        return serde;
    }

    @Override
    public SpecificAvroSerde<CallAggregateKey> CallAggregateKey() {
        SpecificAvroSerde<CallAggregateKey> serde = new SpecificAvroSerde<>();
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, true);
        return serde;
    }

    @Override
    public SpecificAvroSerde<CallAggregateValue> CallAggregateValue() {
        SpecificAvroSerde<CallAggregateValue> serde = new SpecificAvroSerde<>();
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, false);
        return serde;
    }

    @Override
    public SpecificAvroSerde<SmsAggregateKey> SmsAggregateKey() {
        SpecificAvroSerde<SmsAggregateKey> serde = new SpecificAvroSerde<>();
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, true);
        return serde;
    }

    @Override
    public SpecificAvroSerde<SmsAggregateValue> SmsAggregateValue() {
        SpecificAvroSerde<SmsAggregateValue> serde = new SpecificAvroSerde<>();
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, false);
        return serde;
    }*/

    @Override
    public Serde<Windowed<CallAggregateKey>> WindowedCallAggregateKey(Long windowSize) {
        Serde<Windowed<CallAggregateKey>> windowedSerde = new WindowedSerdes.TimeWindowedSerde<>(Key(), windowSize);
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        windowedSerde.configure(config, true);
        return windowedSerde;
    }
}
