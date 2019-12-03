package no.safebase;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroProducerV2 {
    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties);
        String topic = "customer-avro";

        Customer customer = Customer.newBuilder()
                .setFirstName("Mark")
                .setLastName("Bark")
                .setAge(60)
                .setWeight(90.2f)
                .setHeight(182.5f)
                .setPhoneNumber("123-456-789")
                .setEmail("mark.b@gmail.com")
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);

        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null)
                System.out.println("Success!\n" + recordMetadata.toString());
            else
                e.printStackTrace();
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
