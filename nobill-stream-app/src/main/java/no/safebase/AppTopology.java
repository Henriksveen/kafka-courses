package no.safebase;

import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallAggregateValue;
import no.safebase.nobill.model.CallRecordKey;
import no.safebase.nobill.model.CallRecordValue;
import no.safebase.serde.AppSerdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class AppTopology {

    public static void withBuilder(StreamsBuilder streamsBuilder, AppSerdes appSerdes) {

        KStream<CallRecordKey, CallRecordValue> callRecordStream = streamsBuilder
                .stream(AppConfig.NOBILL_CALL_RECORD_TOPIC, Consumed.with(appSerdes.getKeySerde(), appSerdes.getValueSerde()));

        KTable<CallAggregateKey, CallAggregateValue> aggHourTable = callRecordStream
                .selectKey((key, value) -> RecordBuilder.CallAggregateKey(value))
                .groupByKey(Grouped.with(appSerdes.getKeySerde(), appSerdes.getValueSerde()))
                .aggregate(
                        () -> CallAggregateValue.newBuilder().setCalls(0L).setCost(0.0).setDuration(0L).build(),
                        (CallAggregateKey key, CallRecordValue newValue, CallAggregateValue agg) -> {
                            agg.put("calls", agg.getCalls() + 1);
                            agg.put("cost", agg.getCost() + newValue.getCost());
                            agg.put("duration", agg.getDuration() + newValue.getDuration());
                            return agg;
                        }, Materialized.<CallAggregateKey, CallAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR)
                                .withKeySerde(appSerdes.getKeySerde())
                                .withValueSerde(appSerdes.getValueSerde())
//                                .withLoggingEnabled(storeConfig)
                );

        KStream<CallAggregateKey, CallAggregateValue> aggHourStream = aggHourTable.toStream()
                .through(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC, Produced.with(appSerdes.getKeySerde(), appSerdes.getValueSerde()));

        KTable<CallAggregateKey, CallAggregateValue> aggDayTable = aggHourStream
                .selectKey((key, value) -> RecordBuilder.CallAggregateKey(key))
                .groupByKey(Grouped.with(appSerdes.getKeySerde(), appSerdes.getValueSerde()))
                .aggregate(
                        () -> CallAggregateValue.newBuilder().setCalls(0L).setCost(0.0).setDuration(0L).build(),
                        (CallAggregateKey key, CallAggregateValue newValue, CallAggregateValue agg) -> {
                            agg.put("calls", agg.getCalls() + newValue.getCalls());
                            agg.put("cost", agg.getCost() + newValue.getCost());
                            agg.put("duration", agg.getDuration() + newValue.getDuration());
                            return agg;
                        }, Materialized.<CallAggregateKey, CallAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_DAY)
                                .withKeySerde(appSerdes.getKeySerde())
                                .withValueSerde(appSerdes.getValueSerde())
//                                .withLoggingEnabled(storeConfig));
                );

        aggDayTable.toStream().to(AppConfig.NOBILL_CALL_RECORD_DAY_TOPIC, Produced.with(appSerdes.getKeySerde(), appSerdes.getValueSerde()));
    }
}