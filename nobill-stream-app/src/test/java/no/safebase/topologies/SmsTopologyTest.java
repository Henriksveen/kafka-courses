package no.safebase.topologies;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import no.safebase.AggregateType;
import no.safebase.AppConfig;
import no.safebase.nobill.model.*;
import no.safebase.serde.MockNobillSerdes;
import no.safebase.serde.NobillSerdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SmsTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<NobillRecordKey, SmsRecordValue> inputTopic;
    private TestOutputTopic<SmsAggregateKey, SmsAggregateValue> hourOutputTopic;
    private TestOutputTopic<SmsAggregateKey, SmsAggregateValue> dayOutputTopic;

    private NobillSerdes MockSerdes = new MockNobillSerdes(new MockSchemaRegistryClient());

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        AppTopology.withBuilder(streamsBuilder, MockSerdes, AggregateType.SMS);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), props);

        inputTopic = testDriver.createInputTopic(AppConfig.NOBILL_SMS_RECORD_TOPIC,
                MockSerdes.<NobillRecordKey>Key().serializer(), MockSerdes.<SmsRecordValue>Value().serializer());
        hourOutputTopic = testDriver.createOutputTopic(AppConfig.NOBILL_SMS_RECORD_HOUR_TOPIC,
                MockSerdes.<SmsAggregateKey>Key().deserializer(), MockSerdes.<SmsAggregateValue>Value().deserializer());
        dayOutputTopic = testDriver.createOutputTopic(AppConfig.NOBILL_SMS_RECORD_DAY_TOPIC,
                MockSerdes.<SmsAggregateKey>Key().deserializer(), MockSerdes.<SmsAggregateValue>Value().deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }


    @Test
    @DisplayName("Test the SMS topology flow from the source topic through hour to the final output topic.")
    void topologyFlowTest() {
        inputTopic.pipeInput(keyRecordWithId("123"), valueRecordWithStartTime("20200114091500"));

        assertKeyValue(hourOutputTopic.readKeyValue(), 1578988800000L, 2.0, 1L);
        assertTrue(hourOutputTopic.isEmpty());

        assertKeyValue(dayOutputTopic.readKeyValue(), 1578956400000L, 2.0, 1L);
        assertTrue(dayOutputTopic.isEmpty());
    }

    @Test
    @DisplayName("SMS Topology should aggregate correctly HOUR")
    void recordAggregateHourTest() {
        inputTopic.pipeInput(keyRecordWithId("1"), valueRecordWithStartTime("20200114091500"));
        inputTopic.pipeInput(keyRecordWithId("2"), valueRecordWithStartTime("20200114093000"));
        inputTopic.pipeInput(keyRecordWithId("3"), valueRecordWithStartTime("20200114094500"));

        assertKeyValue(hourOutputTopic.readKeyValue(), 1578988800000L, 2.0, 1L);
        hourOutputTopic.readKeyValue();

        assertKeyValue(hourOutputTopic.readKeyValue(), 1578988800000L, 6.0, 3L);
        assertTrue(hourOutputTopic.isEmpty());
    }

    @Test
    @DisplayName("SMS Topology should aggregate correctly DAY")
    void recordAggregateDayTest() {
        inputTopic.pipeInput(keyRecordWithId("1"), valueRecordWithStartTime("20200114091500"));
        inputTopic.pipeInput(keyRecordWithId("2"), valueRecordWithStartTime("20200114103000"));
        inputTopic.pipeInput(keyRecordWithId("3"), valueRecordWithStartTime("20200113144500"));
        inputTopic.pipeInput(keyRecordWithId("4"), valueRecordWithStartTime("20200114114500"));
        inputTopic.pipeInput(keyRecordWithId("5"), valueRecordWithStartTime("20200115114500"));

        assertKeyValue(dayOutputTopic.readKeyValue(), 1578956400000L, 2.0, 1L);

        dayOutputTopic.readKeyValue();
        dayOutputTopic.readKeyValue();

        assertKeyValue(dayOutputTopic.readKeyValue(), 1578956400000L, 6.0, 3L);
        assertKeyValue(dayOutputTopic.readKeyValue(), 1579042800000L, 2.0, 1L);

        assertTrue(dayOutputTopic.isEmpty());
    }

    private void assertKeyValue(KeyValue<SmsAggregateKey, SmsAggregateValue> record, long aggTime, double cost, long calls) {
        assertAll(
                () -> assertEquals(aggTime, record.key.getPeriodStart()),
                () -> assertEquals(cost, record.value.getCost()),
                () -> assertEquals(calls, record.value.getSmsCount())
        );
    }

    private NobillRecordKey keyRecordWithId(String id) {
        return NobillRecordKey.newBuilder().setInternalreferenceid(id).build();
    }

    public SmsRecordValue valueRecordWithStartTime(String startTime) {
        return SmsRecordValue.newBuilder()
                .setInternalreferenceid("123")
                .setCalltype(1)
                .setRateplanname("Rate Plan Name")
                .setCost(2.0)
                .setCreationtime(startTime)
                .setSubscriptiontypename("Type")
                .build();
    }
}