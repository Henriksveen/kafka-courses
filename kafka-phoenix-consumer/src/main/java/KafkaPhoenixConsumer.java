import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


public class KafkaPhoenixConsumer {

    private Logger logger = LoggerFactory.getLogger(KafkaPhoenixConsumer.class.getName());

    private List<Long> epochs = Collections.singletonList(1583362800000L);

    public static void main(String[] args) {
        new KafkaPhoenixConsumer().run();
    }

    public KafkaPhoenixConsumer() {
    }

    public void run() {
        Properties properties = ConfigParser.getProperties();
        PhoenixClient phoenixClient = new PhoenixClient(properties.getProperty(ConfigParser.HBASE_ZOOKEEPER_QUORUM_CONFIG_CONFIG));

        RecordBuilder recordBuilder = new RecordBuilder();
        KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            phoenixClient.close();
            producer.close();
        }));

        String topic = properties.getProperty(ConfigParser.TOPIC_CONFIG);
        String schemaNameKey = properties.getProperty(ConfigParser.SCHEMA_NAME_KEY_CONFIG);
        String schemaNameValue = properties.getProperty(ConfigParser.SCHEMA_NAME_VALUE_CONFIG);
        List<String> primaryKeys = Arrays.asList(properties.getProperty(ConfigParser.PRIMARY_KEYS_CONFIG).split(","));

        try {
            phoenixClient.prepareStatement("SELECT * FROM TUNNEL.TUNNEL_COVERAGE_DAY WHERE MEASTIME = ?");

            for (Long epoch : epochs) {
                phoenixClient.setDate(new Date(epoch));
                ResultSet rs = phoenixClient.executeQuery();

                Schema keySchema = SchemaMapping.keySchema(schemaNameKey, rs.getMetaData(), primaryKeys);
                Schema valueSchema = SchemaMapping.valueSchema(schemaNameValue, rs.getMetaData(), primaryKeys);

                while (rs.next()) {
                    GenericRecord key = recordBuilder.extractRecord(keySchema, rs);
                    GenericRecord value = recordBuilder.extractRecord(valueSchema, rs);
                    ProducerRecord<GenericRecord, GenericRecord> producerRecord = new ProducerRecord<>(topic, key, value);

                    producer.send(producerRecord);
                }
            }
        } catch (SQLException e) {
            logger.error("", e);
            throw new RuntimeException(e);
        }
        logger.info("End of application");
    }
}
