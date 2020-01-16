package no.safebase;

import no.safebase.serde.NobillSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Nobill {

    // TODO:
    // 1. Add functionality to read config from property-file
    // 3. Remove cleanUp(), could be smart to add functionality to have this as an option from property-file
    // 4. Add storeConfig for state store OR use windowed aggregation
    // 5. Remove timeUnit from Aggregate enum //TODO: OR USE SAME STATE STORE??

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfig.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 104857600);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);
//        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 5);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        AppTopology.withBuilder(streamsBuilder, new NobillSerdes());

        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.cleanUp(); // TODO: Remove cleanUp() in prod
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}