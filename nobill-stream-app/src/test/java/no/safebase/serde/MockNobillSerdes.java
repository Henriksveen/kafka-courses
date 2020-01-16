package no.safebase.serde;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

import java.util.HashMap;
import java.util.Map;

public class MockNobillSerdes implements AppSerdes {

    private SchemaRegistryClient registry;

    public MockNobillSerdes(SchemaRegistryClient registry) {
        this.registry = registry;
    }

    @Override
    public <T extends SpecificRecord> SpecificAvroSerde<T> getKeySerde() {
        return getSerde(true);
    }

    @Override
    public <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerde() {
        return getSerde(false);
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> getSerde(boolean isKey) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>(registry);
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://dummy:1234");
        serde.configure(config, isKey);
        return serde;
    }
}
