import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class UserEventEnricherApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        // we get a global table out of Kafka. This table will be replicated on each Kafka Stream application
        // the key of our globalKTable is the user ID
        final GlobalKTable<String, String> userGlobalTable = builder.globalTable("user-table");

        // we get a stream of user purchases
        final KStream<String, String> userPurchases = builder.stream("user-purchases");

        // we want to enrich that stream
        final KStream<String, String> userPurchasesEnrichedJoin = userPurchases.join(userGlobalTable,
                (key, value) -> key, // map from the (key, value) of this stream to the stream of the GlobalKTable
                (userPurchase, userInfo) -> "Purchase=" + userPurchase + " ,UserInfo=[" + userInfo + "]"
        );

        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        // we want to enrich that stream using a Left join
        final KStream<String, String> userPurchasesEnrichedLeftJoin = userPurchases.leftJoin(userGlobalTable,
                (key, value) -> key, // map from the (key, value) of this stream to the key of the GlobalKTable
                (userPurchase, userInfo) -> {
                    if (userInfo != null)
                        return "Purchase=" + userPurchase + " ,UserInfo=[" + userInfo + "]";
                    else
                        return "Purchase=" + userPurchase + " ,UserInfo=null";
                }
        );

        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");
        final KafkaStreams streams = new KafkaStreams(builder.build(), config);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-favourite-colour-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.cleanUp(); // only during development
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
