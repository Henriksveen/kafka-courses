package no.safebase;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallAggregateValue;
import no.safebase.nobill.model.CallRecordKey;
import no.safebase.nobill.model.CallRecordValue;
import no.safebase.serde.NobillSerdes;
import no.safebase.serde.MockNobillSerdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AppTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<CallRecordKey, CallRecordValue> inputTopic;
    private TestOutputTopic<CallAggregateKey, CallAggregateValue> hourOutputTopic;
    private TestOutputTopic<CallAggregateKey, CallAggregateValue> dayOutputTopic;

    private NobillSerdes MockSerdes = new MockNobillSerdes(new MockSchemaRegistryClient());

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        AppTopology.withBuilder(streamsBuilder, MockSerdes);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), props);

        inputTopic = testDriver.createInputTopic(AppConfig.NOBILL_CALL_RECORD_TOPIC,
                MockSerdes.CallRecordKey().serializer(), MockSerdes.CallRecordValue().serializer());
        hourOutputTopic = testDriver.createOutputTopic(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC,
                MockSerdes.CallAggregateKey().deserializer(), MockSerdes.CallAggregateValue().deserializer());
        dayOutputTopic = testDriver.createOutputTopic(AppConfig.NOBILL_CALL_RECORD_DAY_TOPIC,
                MockSerdes.CallAggregateKey().deserializer(), MockSerdes.CallAggregateValue().deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    @DisplayName("Test the topology flow from the source topic through hour to the final output topic.")
    void topologyFlowTest() {
        inputTopic.pipeInput(keyRecordWithId("123"), valueRecordWithStartTime("20200114091500"));

        assertKeyValue(hourOutputTopic.readKeyValue(), 1578988800000L, 2.0, 1L, 1000L);
        assertTrue(hourOutputTopic.isEmpty());

        assertKeyValue(dayOutputTopic.readKeyValue(), 1578956400000L, 2.0, 1L, 1000L);
        assertTrue(dayOutputTopic.isEmpty());
    }

    @Test
    @DisplayName("Topology should aggregate correctly HOUR")
    void recordAggregateHourTest() {
        inputTopic.pipeInput(keyRecordWithId("1"), valueRecordWithStartTime("20200114091500"));
        inputTopic.pipeInput(keyRecordWithId("2"), valueRecordWithStartTime("20200114093000"));
        inputTopic.pipeInput(keyRecordWithId("3"), valueRecordWithStartTime("20200114094500"));

        assertKeyValue(hourOutputTopic.readKeyValue(), 1578988800000L, 2.0, 1L, 1000L);
        hourOutputTopic.readKeyValue();

        assertKeyValue(hourOutputTopic.readKeyValue(), 1578988800000L, 6.0, 3L, 3000L);
        assertTrue(hourOutputTopic.isEmpty());
    }

    @Test
    @DisplayName("Topology should aggregate correctly DAY")
    void recordAggregateDayTest() {
        inputTopic.pipeInput(keyRecordWithId("1"), valueRecordWithStartTime("20200114091500"));
        inputTopic.pipeInput(keyRecordWithId("2"), valueRecordWithStartTime("20200114103000"));
        inputTopic.pipeInput(keyRecordWithId("3"), valueRecordWithStartTime("20200113144500"));
        inputTopic.pipeInput(keyRecordWithId("4"), valueRecordWithStartTime("20200114114500"));
        inputTopic.pipeInput(keyRecordWithId("5"), valueRecordWithStartTime("20200115114500"));

        assertKeyValue(dayOutputTopic.readKeyValue(), 1578956400000L, 2.0, 1L, 1000L);

        dayOutputTopic.readKeyValue();
        dayOutputTopic.readKeyValue();

        assertKeyValue(dayOutputTopic.readKeyValue(), 1578956400000L, 6.0, 3L, 3000L);
        assertKeyValue(dayOutputTopic.readKeyValue(), 1579042800000L, 2.0, 1L, 1000L);

        assertTrue(dayOutputTopic.isEmpty());
    }

    private void assertKeyValue(KeyValue<CallAggregateKey, CallAggregateValue> record, long aggTime, double cost, long calls, long duration) {
        assertAll(
                () -> assertEquals(aggTime, record.key.getPeriodStart()),
                () -> assertEquals(cost, record.value.getCost()),
                () -> assertEquals(calls, record.value.getCalls()),
                () -> assertEquals(duration, record.value.getDuration())
        );
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