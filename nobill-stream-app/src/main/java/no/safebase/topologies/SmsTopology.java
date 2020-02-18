package no.safebase.topologies;

import no.safebase.AppConfig;
import no.safebase.RecordBuilder;
import no.safebase.nobill.model.*;
import no.safebase.serde.NobillSerdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

public class SmsTopology implements Topology {

    @Override
    public void hourAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {
        KStream<NobillRecordKey, SmsRecordValue> KS_0 = streamsBuilder
                .stream(AppConfig.NOBILL_SMS_RECORD_TOPIC,
                        Consumed.with(AppSerdes.<NobillRecordKey>Key(), AppSerdes.<SmsRecordValue>Value()));

        KTable<SmsAggregateKey, SmsAggregateValue> KT_0 = KS_0
                .groupBy((k, v) -> RecordBuilder.SmsAggregateKey(v), Grouped.with(AppSerdes.<SmsAggregateKey>Key(), AppSerdes.<SmsRecordValue>Value()))
                .aggregate(
                        () -> SmsAggregateValue.newBuilder().setSmsCount(0L).setCost(0.0).build(),
                        (SmsAggregateKey key, SmsRecordValue newValue, SmsAggregateValue agg) -> SmsAggregateValue.newBuilder()
                                .setSmsCount(agg.getSmsCount() + 1)
                                .setCost(agg.getCost() + newValue.getCost())
                                .build()
                        , Materialized.<SmsAggregateKey, SmsAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR)
                                .withKeySerde(AppSerdes.<SmsAggregateKey>Key())
                                .withValueSerde(AppSerdes.<SmsAggregateValue>Value())
                );

        KT_0.toStream().print(Printed.<SmsAggregateKey, SmsAggregateValue>toSysOut().withLabel("HOUR"));
        KT_0.toStream()
                .to(AppConfig.NOBILL_SMS_RECORD_HOUR_TOPIC, Produced.with(AppSerdes.<SmsAggregateKey>Key(), AppSerdes.<SmsAggregateValue>Value()));
    }

    @Override
    public void dayAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {

        KTable<SmsAggregateKey, SmsAggregateValue> KT_0 = streamsBuilder
                .table(AppConfig.NOBILL_SMS_RECORD_HOUR_TOPIC, Consumed.with(AppSerdes.<SmsAggregateKey>Key(), AppSerdes.<SmsAggregateValue>Value())
                        , Materialized.<SmsAggregateKey, SmsAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR_CONSUMER)
                                .withKeySerde(AppSerdes.<SmsAggregateKey>Key())
                                .withValueSerde(AppSerdes.<SmsAggregateValue>Value()));

        KTable<SmsAggregateKey, SmsAggregateValue> KT_1 = KT_0
                .groupBy((key, value) -> KeyValue.pair(RecordBuilder.SmsAggregateKey(key), value), Grouped.with(AppSerdes.<SmsAggregateKey>Key(), AppSerdes.<SmsAggregateValue>Value()))
                .aggregate(
                        () -> SmsAggregateValue.newBuilder().setSmsCount(0L).setCost(0.0).build(),
                        (k, newValue, agg) -> SmsAggregateValue.newBuilder()
                                .setSmsCount(agg.getSmsCount() + newValue.getSmsCount())
                                .setCost(agg.getCost() + newValue.getCost())
                                .build()
                        , (k, newValue, agg) -> SmsAggregateValue.newBuilder()
                                .setSmsCount(agg.getSmsCount() - newValue.getSmsCount())
                                .setCost(agg.getCost() - newValue.getCost())
                                .build()
                        , Materialized.<SmsAggregateKey, SmsAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_DAY)
                                .withKeySerde(AppSerdes.<SmsAggregateKey>Key())
                                .withValueSerde(AppSerdes.<SmsAggregateValue>Value())
                );
        KT_1.toStream().print(Printed.<SmsAggregateKey, SmsAggregateValue>toSysOut().withLabel("DAY"));
        KT_1.toStream().to(AppConfig.NOBILL_SMS_RECORD_DAY_TOPIC, Produced.with(AppSerdes.<SmsAggregateKey>Key(), AppSerdes.<SmsAggregateValue>Value()));
    }
}
