package no.safebase;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankProducer {

    private Logger logger = LoggerFactory.getLogger(BankProducer.class.getName());

    public static void main(String[] args) throws InterruptedException {
        new BankProducer().run();
    }

    public BankProducer() {
    }

    public void run() throws InterruptedException {
        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

        List<String> persons = Arrays.asList("stephan", "alice", "mark", "tom", "penny", "john");

        int i = 0;

        while (true) {
            logger.info("Produced item: {}", i++);
            String name = persons.get((int) Math.floor(Math.random() * 6));
            ObjectNode transaction = JsonNodeFactory.instance.objectNode();
            transaction.put("name", name);
            transaction.put("price", ThreadLocalRandom.current().nextInt(0, 100));
            transaction.put("time", Instant.now().toString());
            producer.send(new ProducerRecord<>("bank-transactions", name, transaction.toString()));
            Thread.sleep(100);
        }
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // high throughput producer( at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size

        // create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        return new KafkaProducer<>(properties);
    }
}
