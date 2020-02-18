package no.safebase.topologies;

import no.safebase.AppConfig;
import no.safebase.AppTimestampExtractor;
import no.safebase.RecordBuilder;
import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallAggregateValue;
import no.safebase.nobill.model.NobillRecordKey;
import no.safebase.nobill.model.CallRecordValue;
import no.safebase.serde.NobillSerdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.*;

public class WindowedTestAppTopology {

    private static final Long WINDOW_SIZE_HOUR = 60 * 60 * 1000L;

    public static void withBuilder(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {
//        hourAggregate(streamsBuilder, AppSerdes);
//        dayAggregate(streamsBuilder, AppSerdes);
    }

    static void windowedAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {
        KStream<NobillRecordKey, CallRecordValue> KS_0 = streamsBuilder
                .stream(AppConfig.NOBILL_CALL_RECORD_TOPIC, Consumed.with(AppSerdes.<NobillRecordKey>Key(), AppSerdes.<CallRecordValue>Value())
                        .withTimestampExtractor(new AppTimestampExtractor()));

        KTable<Windowed<CallAggregateKey>, CallAggregateValue> KT_0 = KS_0
                .groupBy((k, v) -> RecordBuilder.CallAggregateKey(v), Grouped.with(AppSerdes.<CallAggregateKey>Key(), AppSerdes.<CallRecordValue>Value()))
                .windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .aggregate(
                        () -> CallAggregateValue.newBuilder().setCallCount(0L).setDuration(0L).setCost(0.0).build()
                        , (CallAggregateKey key, CallRecordValue newValue, CallAggregateValue agg) -> CallAggregateValue.newBuilder()
                                .setCallCount(agg.getCallCount() + 1)
                                .setCost(agg.getCost() + newValue.getCost())
                                .setDuration(agg.getDuration() + newValue.getDuration())
                                .build()
                        , Materialized.<CallAggregateKey, CallAggregateValue, WindowStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR)
                                .withKeySerde(AppSerdes.<CallAggregateKey>Key())
                                .withValueSerde(AppSerdes.<CallAggregateValue>Value())
                );

        KT_0.toStream()
                .to(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC, Produced.with(AppSerdes.WindowedCallAggregateKey(WINDOW_SIZE_HOUR), AppSerdes.<CallAggregateValue>Value()));

//        KT_0.toStream().print(Printed.<Windowed<CallAggregateKey>, CallAggregateValue>toSysOut().withLabel("Call Hour Aggregate"));

        KTable<CallAggregateKey, CallAggregateValue> KT_1 = streamsBuilder
                .table(AppConfig.NOBILL_CALL_RECORD_HOUR_TOPIC,
                        Consumed.with(AppSerdes.<CallAggregateKey>Key(), AppSerdes.<CallAggregateValue>Value()));

        KTable<CallAggregateKey, CallAggregateValue> KT_2 = KT_1
                .groupBy((k, v) -> KeyValue.pair(RecordBuilder.CallAggregateKey(k), v), Grouped.with(AppSerdes.<CallAggregateKey>Key(), AppSerdes.<CallAggregateValue>Value()))
                .aggregate(
                        () -> CallAggregateValue.newBuilder().setCallCount(0L).setDuration(0L).setCost(0.0).build(),
                        (k, newValue, agg) -> CallAggregateValue.newBuilder()
                                .setCallCount(agg.getCallCount() + newValue.getCallCount())
                                .setCost(agg.getCost() + newValue.getCost())
                                .setDuration(agg.getDuration() + newValue.getDuration())
                                .build()
                        , (key, newValue, agg) -> CallAggregateValue.newBuilder()
                                .setCallCount(agg.getCallCount() - newValue.getCallCount())
                                .setCost(agg.getCost() - newValue.getCost())
                                .setDuration(agg.getDuration() - newValue.getDuration())
                                .build()
                        , Materialized.<CallAggregateKey, CallAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_DAY)
                                .withKeySerde(AppSerdes.<CallAggregateKey>Key())
                                .withValueSerde(AppSerdes.<CallAggregateValue>Value())
                );

//        KT_2.toStream().print(Printed.<CallAggregateKey, CallAggregateValue>toSysOut().withLabel("Call Day Aggregate"));
        KT_2.toStream().to(AppConfig.NOBILL_CALL_RECORD_DAY_TOPIC, Produced.with(AppSerdes.<CallAggregateKey>Key(), AppSerdes.<CallAggregateValue>Value()));
    }

}