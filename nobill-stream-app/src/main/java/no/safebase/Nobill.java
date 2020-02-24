package no.safebase;

import no.safebase.serde.AppSerdes;
import no.safebase.topologies.AppTopology;
import org.apache.kafka.streams.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Nobill {

    private static final Logger logger = LoggerFactory.getLogger(Nobill.class);

    // TODO:
    // 1. Add functionality to read config from property-file
    // 3. Remove cleanUp(), could be smart to add functionality to have this as an option from property-file
    // 4. Add storeConfig for state store OR use windowed aggregate
    // 5. Remove timeUnit from Aggregate enum //TODO: OR USE SAME STATE STORE??

    public static void main(String[] args) {
        Properties props = ConfigParser.getProperties();
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        AggregateType type = AggregateType.valueOf(AppConfig.AGGREGATE_TYPE);
        AppTopology.withBuilder(streamsBuilder, new AppSerdes(), type);

        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);

        //Query Server
//        QueryServer queryServer = new QueryServer(streams, AppConfig.queryServerHost, AppConfig.queryServerPort);
//        streams.setStateListener((newState, oldState) -> {
//            logger.info("State Changing to " + newState + " from " + oldState);
//            queryServer.setActive(newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING);
//        });
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
//                queryServer.stop();
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.cleanUp(); // TODO: Remove cleanUp() in prod
            streams.start();
//            queryServer.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}