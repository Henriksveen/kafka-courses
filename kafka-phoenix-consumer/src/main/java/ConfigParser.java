import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class ConfigParser {

    public static String TOPIC_CONFIG = "topic";
    public static String HBASE_ZOOKEEPER_QUORUM_CONFIG_CONFIG = "hbase.zookeeper.quorum";
    public static String SCHEMA_NAME_KEY_CONFIG = "schema.name.key";
    public static String SCHEMA_NAME_VALUE_CONFIG = "schema.name.value";
    public static String PRIMARY_KEYS_CONFIG = "primary.keys";
    public static String TIME_FIELD_CONFIG = "time.field";
    public static String TABLE_NAME_CONFIG = "table.name";
    public static String QUERY_CONFIG = "query";

    private static final Logger log = LoggerFactory.getLogger(ConfigParser.class);

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // high throughput producer( at the expense of a bit of latency and CPU usage)
//        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
//        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size

        // create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        loadConfigFile(properties);

        if (properties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG) == null) {
            log.info("{} is required! [http://host:8081]", AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
            throw new IllegalArgumentException();
        }

        if (properties.getProperty(TOPIC_CONFIG) == null) {
            log.info("{} is required! [topic-name]", TOPIC_CONFIG);
            throw new IllegalArgumentException();
        }

        if (properties.getProperty(HBASE_ZOOKEEPER_QUORUM_CONFIG_CONFIG) == null) {
            log.info("{} is required! [host1,host2]", HBASE_ZOOKEEPER_QUORUM_CONFIG_CONFIG);
            throw new IllegalArgumentException();
        }

        if (properties.getProperty(SCHEMA_NAME_KEY_CONFIG) == null) {
            log.info("{} is required! [schema_name_key] - some characters is not allowed - use example-format", SCHEMA_NAME_KEY_CONFIG);
            throw new IllegalArgumentException();
        }

        if (properties.getProperty(SCHEMA_NAME_VALUE_CONFIG) == null) {
            log.info("{} is required! [schema_name_value] - some characters is not allowed - use example-format", SCHEMA_NAME_VALUE_CONFIG);
            throw new IllegalArgumentException();
        }

        if (properties.getProperty(PRIMARY_KEYS_CONFIG) == null) {
            log.info("{} is required! [column1,column2,column3]", PRIMARY_KEYS_CONFIG);
            throw new IllegalArgumentException();
        }

        if (properties.getProperty(TIME_FIELD_CONFIG) == null) {
            log.info("{} is required! [time_field] - typical the row_timestamp column", TIME_FIELD_CONFIG);
            throw new IllegalArgumentException();
        }

        if (properties.getProperty(TABLE_NAME_CONFIG) == null && properties.getProperty(QUERY_CONFIG) == null) {
            log.info("Either {} or {} is required!", TABLE_NAME_CONFIG, QUERY_CONFIG);
            throw new IllegalArgumentException();
        }

        return properties;
    }

    private static void loadConfigFile(Properties props) {
        try {
            String conf = System.getProperty("conf");
            if (conf != null)
                props.load(new FileInputStream(new File(conf)));
            else
                props.load(Objects.requireNonNull(ConfigParser.class.getClassLoader().getResourceAsStream("config.properties")));
        } catch (IOException e) {
            log.error("Failed loading properties file", e);
        }
    }
}
