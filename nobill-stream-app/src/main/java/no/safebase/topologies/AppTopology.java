package no.safebase.topologies;

import no.safebase.AggregateType;
import no.safebase.serde.NobillSerdes;
import org.apache.kafka.streams.StreamsBuilder;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class AppTopology {
    public static void withBuilder(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes, AggregateType type) {
        Topology topology;
        switch (type) {
            case CALL:
                topology = new CallTopology();
                break;
            case SMS:
                topology = new SmsTopology();
                break;
            case PADS:
                topology = new PadsTopology();
                break;
            default:
                throw new NotImplementedException();
        }

        topology.hourAggregate(streamsBuilder, AppSerdes);
        topology.dayAggregate(streamsBuilder, AppSerdes);
    }
}
