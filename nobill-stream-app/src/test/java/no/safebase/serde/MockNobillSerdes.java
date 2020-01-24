package no.safebase.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import no.safebase.AppConfig;
import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallAggregateValue;
import no.safebase.nobill.model.CallRecordKey;
import no.safebase.nobill.model.CallRecordValue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.util.HashMap;
import java.util.Map;

public class MockNobillSerdes implements NobillSerdes {

    private SchemaRegistryClient registry;

    public MockNobillSerdes(SchemaRegistryClient registry) {
        this.registry = registry;
    }

    @Override
    public SpecificAvroSerde<CallRecordKey> CallRecordKey() {
        SpecificAvroSerde<CallRecordKey> serde = new SpecificAvroSerde<>(registry);
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, true);
        return serde;
    }

    @Override
    public SpecificAvroSerde<CallRecordValue> CallRecordValue() {
        SpecificAvroSerde<CallRecordValue> serde = new SpecificAvroSerde<>(registry);
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, false);
        return serde;
    }

    @Override
    public SpecificAvroSerde<CallAggregateKey> CallAggregateKey() {
        SpecificAvroSerde<CallAggregateKey> serde = new SpecificAvroSerde<>(registry);
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, true);
        return serde;
    }

    @Override
    public Serde<Windowed<CallAggregateKey>> WindowedCallAggregateKey(Long windowSize) {
        Serde<Windowed<CallAggregateKey>> windowedSerde = new WindowedSerdes.TimeWindowedSerde<>(CallAggregateKey(), windowSize);
        Map<String, Object> config = new HashMap<>();
        windowedSerde.configure(config, true);
        return windowedSerde;
    }


    @Override
    public SpecificAvroSerde<CallAggregateValue> CallAggregateValue() {
        SpecificAvroSerde<CallAggregateValue> serde = new SpecificAvroSerde<>(registry);
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, false);
        return serde;
    }
}