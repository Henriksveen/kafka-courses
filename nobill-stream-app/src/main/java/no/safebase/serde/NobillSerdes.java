package no.safebase.serde;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import no.safebase.AppConfig;
import org.apache.avro.specific.SpecificRecord;

import java.util.HashMap;
import java.util.Map;

public class NobillSerdes implements AppSerdes {

    @Override
    public <T extends SpecificRecord> SpecificAvroSerde<T> getKeySerde() {
        return getSerde(true);
    }

    @Override
    public <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerde() {
        return getSerde(false);
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> getSerde(boolean isKey) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        Map<String, Object> config = new HashMap<>();

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfig.SCHEMA_REGISTRY);
        serde.configure(config, isKey);
        return serde;
    }
}
