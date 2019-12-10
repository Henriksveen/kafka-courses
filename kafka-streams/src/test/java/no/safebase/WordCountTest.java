package no.safebase;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class WordCountTest {

    private TopologyTestDriver testDriver;
    private StringSerializer stringSerializer = new StringSerializer();
    private ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

    @BeforeEach
    void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        WordCount wordCount = new WordCount();
        Topology topology = wordCount.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    private void pushNewInputRecord(String value) {
        testDriver.pipeInput(recordFactory.create("streams-plaintext-input", null, value));
    }

    private ProducerRecord<String, Long> readOutput() {
        return testDriver.readOutput("streams-wordcount-output", new StringDeserializer(), new LongDeserializer());
    }

    @Test
    void makeSureCountsAreCorrect() {
        String firstExample = "testing Kafka Streams";
        pushNewInputRecord(firstExample);

        OutputVerifier.compareKeyValue(readOutput(), "testing", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "streams", 1L);
        assertNull(readOutput());

        String secondExample = "testing Kafka again";
        pushNewInputRecord(secondExample);

        OutputVerifier.compareKeyValue(readOutput(), "testing", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "again", 1L);
        assertNull(readOutput());
    }

    @Test
    void makeSureWordsBecomeLowercase() {
        String upperCaseString = "KAFKA kafka Kafka";
        pushNewInputRecord(upperCaseString);

        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 2L);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 3L);
        assertNull(readOutput());
    }
}
