package no.safebase.topologies;

import no.safebase.AppConfig;
import no.safebase.RecordBuilder;
import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallAggregateValue;
import no.safebase.nobill.model.CallRecordValue;
import no.safebase.nobill.model.NobillRecordKey;
import no.safebase.serde.NobillSerdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class CallTopology implements Topology {
    @Override
    public void hourAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {
        KStream<NobillRecordKey, CallRecordValue> KS_0 = streamsBuilder
                .stream(AppConfig.NOBILL_CALL_RECORD_TOPIC,
                        Consumed.with(AppSerdes.<NobillRecordKey>Key(), AppSerdes.<CallRecordValue>Value()));

        KTable<CallAggregateKey, CallAggregateValue> KT_0 = KS_0
                .groupBy((k, v) -> RecordBuilder.CallAggregateKey(v), Grouped.with(AppSerdes.<CallAggregateKey>Key(), AppSerdes.<CallRecordValue>Value()))
                .aggregate(
                        () -> CallAggregateValue.newBuilder().setCallCount(0L).setCost(0.0).setDuration(0L).build(),
                        (CallAggregateKey key, CallRecordValue newValue, CallAggregateValue agg) -> CallAggregateValue.newBuilder()
                                .setCallCount(agg.getCallCount() + 1)
                                .setCost(agg.getCost() + newValue.getCost())
                                .setDuration(agg.getDuration() + newValue.getDuration())
                                .build()
                        , Materialized.<CallAggregateKey, CallAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR)
                                .withKeySerde(AppSerdes.<CallAggregateKey>Key())
                                .withValueSerde(AppSerdes.<CallAggregateValue>Value())
                );

        KT_0.toStream().print(Printed.<CallAggregateKey, CallAggregateValue>toSysOut().withLabel("HOUR"));
        KT_0.toStream()
                .to(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC, Produced.with(AppSerdes.<CallAggregateKey>Key(), AppSerdes.<CallAggregateValue>Value()));
    }

    @Override
    public void dayAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {
        KTable<CallAggregateKey, CallAggregateValue> KT_0 = streamsBuilder
                .table(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC, Consumed.with(AppSerdes.<CallAggregateKey>Key(), AppSerdes.<CallAggregateValue>Value())
                        , Materialized.<CallAggregateKey, CallAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR_CONSUMER)
                                .withKeySerde(AppSerdes.<CallAggregateKey>Key())
                                .withValueSerde(AppSerdes.<CallAggregateValue>Value()));

        KTable<CallAggregateKey, CallAggregateValue> KT_1 = KT_0
                .groupBy((key, value) -> KeyValue.pair(RecordBuilder.CallAggregateKey(key), value), Grouped.with(AppSerdes.<CallAggregateKey>Key(), AppSerdes.<CallAggregateValue>Value()))
                .aggregate(
                        () -> CallAggregateValue.newBuilder().setCallCount(0L).setCost(0.0).setDuration(0L).build(),
                        (k, newValue, agg) -> CallAggregateValue.newBuilder()
                                .setCallCount(agg.getCallCount() + newValue.getCallCount())
                                .setCost(agg.getCost() + newValue.getCost())
                                .setDuration(agg.getDuration() + newValue.getDuration())
                                .build()
                        , (k, newValue, agg) -> CallAggregateValue.newBuilder()
                                .setCallCount(agg.getCallCount() - newValue.getCallCount())
                                .setCost(agg.getCost() - newValue.getCost())
                                .setDuration(agg.getDuration() - newValue.getDuration())
                                .build()
                        , Materialized.<CallAggregateKey, CallAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_DAY)
                                .withKeySerde(AppSerdes.<CallAggregateKey>Key())
                                .withValueSerde(AppSerdes.<CallAggregateValue>Value())
                );
        KT_1.toStream().print(Printed.<CallAggregateKey, CallAggregateValue>toSysOut().withLabel("DAY"));
        KT_1.toStream().to(AppConfig.NOBILL_CALL_RECORD_DAY_TOPIC, Produced.with(AppSerdes.<CallAggregateKey>Key(), AppSerdes.<CallAggregateValue>Value()));
    }
}
