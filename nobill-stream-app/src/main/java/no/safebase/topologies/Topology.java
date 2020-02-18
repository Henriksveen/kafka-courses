package no.safebase.topologies;

import no.safebase.serde.NobillSerdes;
import org.apache.kafka.streams.StreamsBuilder;

public interface Topology {

    void hourAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes);

    void dayAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes);
}
