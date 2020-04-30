import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class KafkaPhoenixProducer {

    private Logger logger = LoggerFactory.getLogger(KafkaPhoenixProducer.class.getName());

    public static void main(String[] args) {
        new KafkaPhoenixProducer().run();
    }

    public KafkaPhoenixProducer() {
    }

    public void run() {
        Properties properties = ConfigParser.getProperties();
        PhoenixClient phoenixClient = new PhoenixClient(properties);

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


        while (!phoenixClient.isDone()) {
            try {
                ResultSet rs;
                if (properties.getProperty(ConfigParser.QUERY_CONFIG) != null)
                    rs = phoenixClient.customResultSet(properties.getProperty(ConfigParser.QUERY_CONFIG));
                else
                    rs = phoenixClient.nextResultSet();


                if (rs != null) {
                    Schema keySchema = SchemaMapping.keySchema(schemaNameKey, rs.getMetaData(), primaryKeys);
                    Schema valueSchema = SchemaMapping.valueSchema(schemaNameValue, rs.getMetaData(), primaryKeys);

                    int i = 0;
                    while (rs.next()) {
                        GenericRecord key = recordBuilder.extractRecord(keySchema, rs);
                        GenericRecord value = recordBuilder.extractRecord(valueSchema, rs);
                        ProducerRecord<GenericRecord, GenericRecord> producerRecord = new ProducerRecord<>(topic, key, value);

                        i++;
                        producer.send(producerRecord);
                    }
                    logger.info("Produced {} records", i);
                    if (properties.getProperty(ConfigParser.QUERY_CONFIG) == null)
                        phoenixClient.archive();
                    rs.close();
                }

            } catch (SQLException e) {
                logger.error("", e);
                throw new RuntimeException(e);
            }
        }
        logger.info("End Application");
    }
}
