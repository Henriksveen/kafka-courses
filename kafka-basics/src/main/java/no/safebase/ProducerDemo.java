package no.safebase;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static final String bootstrapServers = "localhost:9092";

    public static void main(String[] args) {

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

        // send date - asynchronous
        producer.send(record);

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
