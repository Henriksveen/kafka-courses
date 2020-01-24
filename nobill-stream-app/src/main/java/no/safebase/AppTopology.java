package no.safebase;

import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallAggregateValue;
import no.safebase.nobill.model.CallRecordKey;
import no.safebase.nobill.model.CallRecordValue;
import no.safebase.serde.NobillSerdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.*;

public class AppTopology {

    private static final Long WINDOW_SIZE_HOUR = 60 * 60 * 1000L;

    public static void withBuilder(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {
        hourAggregate(streamsBuilder, AppSerdes);
        dayAggregate(streamsBuilder, AppSerdes);
    }

    static void windowedAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {
        KStream<CallRecordKey, CallRecordValue> KS_0 = streamsBuilder
                .stream(AppConfig.NOBILL_CALL_RECORD_TOPIC, Consumed.with(AppSerdes.CallRecordKey(), AppSerdes.CallRecordValue())
                        .withTimestampExtractor(new AppTimestampExtractor()));

        KTable<Windowed<CallAggregateKey>, CallAggregateValue> KT_0 = KS_0
                .groupBy((k, v) -> RecordBuilder.CallAggregateKey(v), Grouped.with(AppSerdes.CallAggregateKey(), AppSerdes.CallRecordValue()))
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .aggregate(
                        () -> CallAggregateValue.newBuilder().setCalls(0L).setDuration(0L).setCost(0.0).build()
                        , (CallAggregateKey key, CallRecordValue newValue, CallAggregateValue agg) -> CallAggregateValue.newBuilder()
                                .setCalls(agg.getCalls() + 1)
                                .setCost(agg.getCost() + newValue.getCost())
                                .setDuration(agg.getDuration() + newValue.getDuration())
                                .build()
                        , Materialized.<CallAggregateKey, CallAggregateValue, WindowStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR)
                                .withKeySerde(AppSerdes.CallAggregateKey())
                                .withValueSerde(AppSerdes.CallAggregateValue())
                );

        KT_0.toStream()
                .to(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC, Produced.with(AppSerdes.WindowedCallAggregateKey(WINDOW_SIZE_HOUR), AppSerdes.CallAggregateValue()));

//        KT_0.toStream().print(Printed.<Windowed<CallAggregateKey>, CallAggregateValue>toSysOut().withLabel("Call Hour Aggregate"));

        KTable<CallAggregateKey, CallAggregateValue> KT_1 = streamsBuilder
                .table(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC,
                        Consumed.with(AppSerdes.CallAggregateKey(), AppSerdes.CallAggregateValue()));

        KTable<CallAggregateKey, CallAggregateValue> KT_2 = KT_1
                .groupBy((k, v) -> KeyValue.pair(RecordBuilder.CallAggregateKey(k), v), Grouped.with(AppSerdes.CallAggregateKey(), AppSerdes.CallAggregateValue()))
                .aggregate(
                        () -> CallAggregateValue.newBuilder().setCalls(0L).setDuration(0L).setCost(0.0).build(),
                        (k, newValue, agg) -> CallAggregateValue.newBuilder()
                                .setCalls(agg.getCalls() + newValue.getCalls())
                                .setCost(agg.getCost() + newValue.getCost())
                                .setDuration(agg.getDuration() + newValue.getDuration())
                                .build()
                        , (key, newValue, agg) -> CallAggregateValue.newBuilder()
                                .setCalls(agg.getCalls() - newValue.getCalls())
                                .setCost(agg.getCost() - newValue.getCost())
                                .setDuration(agg.getDuration() - newValue.getDuration())
                                .build()
                        , Materialized.<CallAggregateKey, CallAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_DAY)
                                .withKeySerde(AppSerdes.CallAggregateKey())
                                .withValueSerde(AppSerdes.CallAggregateValue())
                );

//        KT_2.toStream().print(Printed.<CallAggregateKey, CallAggregateValue>toSysOut().withLabel("Call Day Aggregate"));
        KT_2.toStream().to(AppConfig.NOBILL_CALL_RECORD_DAY_TOPIC, Produced.with(AppSerdes.CallAggregateKey(), AppSerdes.CallAggregateValue()));
    }

    static void hourAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {

        KStream<CallRecordKey, CallRecordValue> KS_0 = streamsBuilder
                .stream(AppConfig.NOBILL_CALL_RECORD_TOPIC,
                        Consumed.with(AppSerdes.CallRecordKey(), AppSerdes.CallRecordValue()));

        KTable<CallAggregateKey, CallAggregateValue> KT_0 = KS_0
                .groupBy((k, v) -> RecordBuilder.CallAggregateKey(v), Grouped.with(AppSerdes.CallAggregateKey(), AppSerdes.CallRecordValue()))
                .aggregate(
                        () -> CallAggregateValue.newBuilder().setCalls(0L).setCost(0.0).setDuration(0L).build(),
                        (CallAggregateKey key, CallRecordValue newValue, CallAggregateValue agg) -> CallAggregateValue.newBuilder()
                                .setCalls(agg.getCalls() + 1)
                                .setCost(agg.getCost() + newValue.getCost())
                                .setDuration(agg.getDuration() + newValue.getDuration())
                                .build()
                        , Materialized.<CallAggregateKey, CallAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR)
                                .withKeySerde(AppSerdes.CallAggregateKey())
                                .withValueSerde(AppSerdes.CallAggregateValue())
                );

        KT_0.toStream()
                .to(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC, Produced.with(AppSerdes.CallAggregateKey(), AppSerdes.CallAggregateValue()));
    }

    static void dayAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {

        KTable<CallAggregateKey, CallAggregateValue> KT_0 = streamsBuilder
                .table(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC, Consumed.with(AppSerdes.CallAggregateKey(), AppSerdes.CallAggregateValue())
                        , Materialized.<CallAggregateKey, CallAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR_CONSUMER)
                                .withKeySerde(AppSerdes.CallAggregateKey())
                                .withValueSerde(AppSerdes.CallAggregateValue()));

        KTable<CallAggregateKey, CallAggregateValue> KT_1 = KT_0
                .groupBy((key, value) -> KeyValue.pair(RecordBuilder.CallAggregateKey(key), value), Grouped.with(AppSerdes.CallAggregateKey(), AppSerdes.CallAggregateValue()))
                .aggregate(
                        () -> CallAggregateValue.newBuilder().setCalls(0L).setCost(0.0).setDuration(0L).build(),
                        (k, newValue, agg) -> CallAggregateValue.newBuilder()
                                .setCalls(agg.getCalls() + newValue.getCalls())
                                .setCost(agg.getCost() + newValue.getCost())
                                .setDuration(agg.getDuration() + newValue.getDuration())
                                .build()
                        , (k, newValue, agg) -> CallAggregateValue.newBuilder()
                                .setCalls(agg.getCalls() - newValue.getCalls())
                                .setCost(agg.getCost() - newValue.getCost())
                                .setDuration(agg.getDuration() - newValue.getDuration())
                                .build()
                        , Materialized.<CallAggregateKey, CallAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_DAY)
                                .withKeySerde(AppSerdes.CallAggregateKey())
                                .withValueSerde(AppSerdes.CallAggregateValue())
                );
        KT_1.toStream().to(AppConfig.NOBILL_CALL_RECORD_DAY_TOPIC, Produced.with(AppSerdes.CallAggregateKey(), AppSerdes.CallAggregateValue()));
    }
}