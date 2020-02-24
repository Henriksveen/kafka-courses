package no.safebase.topologies;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import no.safebase.AggregateType;
import no.safebase.AppConfig;
import no.safebase.nobill.model.PadsAggregateKey;
import no.safebase.nobill.model.PadsAggregateValue;
import no.safebase.nobill.model.PadsRecordValue;
import no.safebase.nobill.model.NobillRecordKey;
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

public class PadsTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<NobillRecordKey, PadsRecordValue> inputTopic;
    private TestOutputTopic<PadsAggregateKey, PadsAggregateValue> hourOutputTopic;
    private TestOutputTopic<PadsAggregateKey, PadsAggregateValue> dayOutputTopic;

    private NobillSerdes MockSerdes = new MockNobillSerdes(new MockSchemaRegistryClient());

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        AppTopology.withBuilder(streamsBuilder, MockSerdes, AggregateType.PADS);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), props);

        inputTopic = testDriver.createInputTopic(AppConfig.NOBILL_PADS_RECORD_TOPIC,
                MockSerdes.<NobillRecordKey>Key().serializer(), MockSerdes.<PadsRecordValue>Value().serializer());
        hourOutputTopic = testDriver.createOutputTopic(AppConfig.NOBILL_PADS_RECORD_HOUR_TOPIC,
                MockSerdes.<PadsAggregateKey>Key().deserializer(), MockSerdes.<PadsAggregateValue>Value().deserializer());
        dayOutputTopic = testDriver.createOutputTopic(AppConfig.NOBILL_PADS_RECORD_DAY_TOPIC,
                MockSerdes.<PadsAggregateKey>Key().deserializer(), MockSerdes.<PadsAggregateValue>Value().deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    @DisplayName("Test the PADS topology flow from the source topic through hour to the final output topic.")
    void topologyFlowTest() {
        inputTopic.pipeInput(keyRecordWithId("123"), valueRecordWithStartTime("20200114091500"));

        assertKeyValue(hourOutputTopic.readKeyValue(), 1578988800000L, 2.0, 1L, 10L, 10L, 1, 123L, 122L, 1000L);
        assertTrue(hourOutputTopic.isEmpty());

        assertKeyValue(dayOutputTopic.readKeyValue(), 1578956400000L, 2.0, 1L, 10L, 10L, 1, 123L, 122L,1000L);
        assertTrue(dayOutputTopic.isEmpty());
    }

    @Test
    @DisplayName("PADS Topology should aggregate correctly HOUR")
    void recordAggregateHourTest() {
        inputTopic.pipeInput(keyRecordWithId("1"), valueRecordWithStartTime("20200114091500"));
        inputTopic.pipeInput(keyRecordWithId("2"), valueRecordWithStartTime("20200114093000"));
        inputTopic.pipeInput(keyRecordWithId("3"), valueRecordWithStartTime("20200114094500"));

        assertKeyValue(hourOutputTopic.readKeyValue(), 1578988800000L, 2.0, 1L, 10L, 10L, 1, 123L, 122L,1000L);
        hourOutputTopic.readKeyValue();

        assertKeyValue(hourOutputTopic.readKeyValue(), 1578988800000L, 6.0, 3L, 30L, 30L, 1, 369L, 366L,3000L);
        assertTrue(hourOutputTopic.isEmpty());
    }

    @Test
    @DisplayName("PADS Topology should aggregate correctly DAY")
    void recordAggregateDayTest() {
        inputTopic.pipeInput(keyRecordWithId("1"), valueRecordWithStartTime("20200114091500"));
        inputTopic.pipeInput(keyRecordWithId("2"), valueRecordWithStartTime("20200114103000"));
        inputTopic.pipeInput(keyRecordWithId("3"), valueRecordWithStartTime("20200113144500"));
        inputTopic.pipeInput(keyRecordWithId("4"), valueRecordWithStartTime("20200114114500"));
        inputTopic.pipeInput(keyRecordWithId("5"), valueRecordWithStartTime("20200115114500"));

        assertKeyValue(dayOutputTopic.readKeyValue(), 1578956400000L, 2.0, 1L, 10L, 10L, 1, 123L, 122L,1000L);

        dayOutputTopic.readKeyValue();
        dayOutputTopic.readKeyValue();

        assertKeyValue(dayOutputTopic.readKeyValue(), 1578956400000L, 6.0, 3L, 30L, 30L, 1, 369L, 366L,3000L);
        assertKeyValue(dayOutputTopic.readKeyValue(), 1579042800000L, 2.0, 1L, 10L, 10L, 1, 123L, 122L,1000L);

        assertTrue(dayOutputTopic.isEmpty());
    }

    private void assertKeyValue(KeyValue<PadsAggregateKey, PadsAggregateValue> record, long aggTime, double cost, long pads, long usedQ, long chargedQ, int unit, long dataSent, long dataRec, long duration) {
        assertAll(
                () -> assertEquals(aggTime, record.key.getPeriodStart()),
                () -> assertEquals(cost, record.value.getCost()),
                () -> assertEquals(pads, record.value.getPadsCount()),
                () -> assertEquals(usedQ, record.value.getUsedQuota()),
                () -> assertEquals(chargedQ, record.value.getChargedQuota()),
                () -> assertEquals(unit, record.value.getQuotaUnit()),
                () -> assertEquals(dataSent, record.value.getDataVolumeSent()),
                () -> assertEquals(dataRec, record.value.getDataVolumeReceived()),
                () -> assertEquals(duration, record.value.getDuration())
        );
    }

    private NobillRecordKey keyRecordWithId(String id) {
        return NobillRecordKey.newBuilder().setInternalreferenceid(id).build();
    }

    public PadsRecordValue valueRecordWithStartTime(String startTime) {
        return PadsRecordValue.newBuilder()
                .setInternalreferenceid("123")
                .setCalltype(1)
                .setTerminationreason(1)
                .setRateplanname("Rate Plan Name")
                .setCost(2.0)
                .setStarttime(startTime)
                .setDuration(1000L)
                .setUsedquota(10L)
                .setChargedquota(10L)
                .setQuotaunit(1)
                .setDatavolumesent(123L)
                .setDatavolumereceived(122L)
                .build();
    }
}
