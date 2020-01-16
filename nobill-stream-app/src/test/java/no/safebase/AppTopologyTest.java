package no.safebase;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallAggregateValue;
import no.safebase.nobill.model.CallRecordKey;
import no.safebase.nobill.model.CallRecordValue;
import no.safebase.serde.AppSerdes;
import no.safebase.serde.MockNobillSerdes;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AppTopologyTest {

    private TopologyTestDriver testDriver;
    private ConsumerRecordFactory<CallRecordKey, CallRecordValue> recordFactory;

    private AppSerdes mockSerdes = new MockNobillSerdes(new MockSchemaRegistryClient());

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        SpecificAvroSerde<CallRecordKey> key = mockSerdes.getKeySerde();
        SpecificAvroSerde<CallRecordValue> value = mockSerdes.getKeySerde();

        recordFactory = new ConsumerRecordFactory<>(key.serializer(), value.serializer());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        AppTopology.withBuilder(streamsBuilder, mockSerdes);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), props);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    @DisplayName("Test the topology flow from the source topic through hour to the final output topic.")
    void topologyFlowTest() {
        produceRecord(keyRecordWithId("123"), valueRecordWithStartTime("20200114091500"));

        final ProducerRecord<CallAggregateKey, CallAggregateValue> hourRecord = readOutput(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC);
        final ProducerRecord<CallAggregateKey, CallAggregateValue> dayRecord = readOutput(AppConfig.NOBILL_CALL_RECORD_DAY_TOPIC);

        assertAll("Testing hour output",
                () -> assertEquals(1578988800000L, hourRecord.key().getAggTime()),
                () -> assertEquals(2.0, hourRecord.value().getCost())
        );

        assertAll("Testing day output",
                () -> assertEquals(1578956400000L, dayRecord.key().getAggTime()),
                () -> assertEquals(2.0, dayRecord.value().getCost())
        );
    }

    @Test
    @DisplayName("Topology should aggregate correctly HOUR")
    void recordAggregateHourTest() {
        String hourTopic = AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC;
        produceRecord(keyRecordWithId("1"), valueRecordWithStartTime("20200114091500"));
        produceRecord(keyRecordWithId("2"), valueRecordWithStartTime("20200114093000"));
        produceRecord(keyRecordWithId("3"), valueRecordWithStartTime("20200114094500"));

        final ProducerRecord<CallAggregateKey, CallAggregateValue> record = readOutput(hourTopic);

        assertAll(
                () -> assertEquals(1578988800000L, record.key().getAggTime()),
                () -> assertEquals(2.0, record.value().getCost())
        );

        readOutput(hourTopic);

        final ProducerRecord<CallAggregateKey, CallAggregateValue> record2 = readOutput(hourTopic);

        assertAll(
                () -> assertEquals(1578988800000L, record2.key().getAggTime()),
                () -> assertEquals(6.0, record2.value().getCost()),
                () -> assertEquals(3, record2.value().getCalls()),
                () -> assertEquals(3000, record2.value().getDuration())
        );

        assertNull(readOutput(hourTopic));
    }

    @Test
    @DisplayName("Topology should aggregate correctly DAY")
    void recordAggregateDayTest() {
        String dayTopic = AppConfig.NOBILL_CALL_RECORD_DAY_TOPIC;
        produceRecord(keyRecordWithId("1"), valueRecordWithStartTime("20200114091500"));
        produceRecord(keyRecordWithId("2"), valueRecordWithStartTime("20200114103000"));
        produceRecord(keyRecordWithId("6"), valueRecordWithStartTime("20200113144500")); // not in same day scope
        produceRecord(keyRecordWithId("3"), valueRecordWithStartTime("20200114114500"));
        produceRecord(keyRecordWithId("4"), valueRecordWithStartTime("20200114124500"));
        produceRecord(keyRecordWithId("5"), valueRecordWithStartTime("20200114134500"));


        final ProducerRecord<CallAggregateKey, CallAggregateValue> record = readOutput(dayTopic);

        assertAll(
                () -> assertEquals(1578956400000L, record.key().getAggTime()),
                () -> assertEquals(2.0, record.value().getCost()),
                () -> assertEquals(1, record.value().getCalls()),
                () -> assertEquals(1000L, record.value().getDuration())
        );

        readOutput(dayTopic);
        readOutput(dayTopic);
        readOutput(dayTopic);
        readOutput(dayTopic);

        final ProducerRecord<CallAggregateKey, CallAggregateValue> record2 = readOutput(dayTopic);

        assertAll(
                () -> assertEquals(1578956400000L, record2.key().getAggTime()),
                () -> assertEquals(10.0, record2.value().getCost()),
                () -> assertEquals(5, record2.value().getCalls()),
                () -> assertEquals(5000L, record2.value().getDuration())
        );

        assertNull(readOutput(dayTopic));
    }


    private ProducerRecord<CallAggregateKey, CallAggregateValue> readOutput(String topic) {
        SpecificAvroSerde<CallAggregateKey> key = mockSerdes.getKeySerde();
        SpecificAvroSerde<CallAggregateValue> value = mockSerdes.getKeySerde();
        return testDriver.readOutput(topic, key.deserializer(), value.deserializer());
    }

    private void produceRecord(CallRecordKey key, CallRecordValue value) {
        testDriver.pipeInput(recordFactory.create(AppConfig.NOBILL_CALL_RECORD_TOPIC, key, value));
    }

    private CallRecordKey keyRecordWithId(String id) {
        return CallRecordKey.newBuilder().setInternalreferenceid(id).build();
    }

    public CallRecordValue valueRecordWithStartTime(String startTime) {
        return CallRecordValue.newBuilder()
                .setInternalreferenceid("123")
                .setCalltype(1)
                .setTerminationreason(1)
                .setRateplanname("Rate Plan Name")
                .setCost(2.0)
                .setStarttime(startTime)
                .setDuration(1000L)
                .build();
    }
}