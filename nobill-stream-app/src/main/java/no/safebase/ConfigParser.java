package no.safebase;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class ConfigParser {

    private static final Logger logger = LoggerFactory.getLogger(ConfigParser.class);
    public static Properties getProperties(){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfig.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1000000);
//        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        loadConfigFile(props);
        return props;
    }

    private static void loadConfigFile(Properties props) {
        try {
            String conf = System.getProperty("conf");
            if (conf != null)
                props.load(new FileInputStream(new File(conf)));
            else
                props.load(Objects.requireNonNull(ConfigParser.class.getClassLoader().getResourceAsStream("config.properties")));
        } catch (IOException e) {
            logger.error("Failed loading properties file", e);
        }
    }
}
