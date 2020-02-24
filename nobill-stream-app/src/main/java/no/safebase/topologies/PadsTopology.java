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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PadsTopology implements Topology {

    private static final Logger logger = LoggerFactory.getLogger(PadsTopology.class);

    @Override
    public void hourAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {
        KStream<NobillRecordKey, PadsRecordValue> KS_0 = streamsBuilder
                .stream(AppConfig.NOBILL_PADS_RECORD_TOPIC,
                        Consumed.with(AppSerdes.<NobillRecordKey>Key(), AppSerdes.<PadsRecordValue>Value()));

        KTable<PadsAggregateKey, PadsAggregateValue> KT_0 = KS_0
                .groupBy((k, v) -> RecordBuilder.PadsAggregateKey(v), Grouped.with(AppSerdes.<PadsAggregateKey>Key(), AppSerdes.<PadsRecordValue>Value()))
                .aggregate(
                        () -> PadsAggregateValue.newBuilder().setPadsCount(0L).setCost(0.0).setUsedQuota(0L).setChargedQuota(0L).setDataVolumeSent(0L).setDataVolumeReceived(0L).setDuration(0L).build(),
                        (PadsAggregateKey key, PadsRecordValue newValue, PadsAggregateValue agg) -> PadsAggregateValue.newBuilder()
                                .setPadsCount(agg.getPadsCount() + 1)
                                .setCost(agg.getCost() + newValue.getCost())
                                .setUsedQuota(agg.getUsedQuota() + newValue.getUsedquota())
                                .setChargedQuota(agg.getChargedQuota() + newValue.getChargedquota())
                                .setQuotaUnit(test(agg.getQuotaUnit(), newValue.getQuotaunit()))
                                .setDataVolumeSent(agg.getDataVolumeSent() + newValue.getDatavolumesent())
                                .setDataVolumeReceived(agg.getDataVolumeReceived() + newValue.getDatavolumereceived())
                                .setDuration(agg.getDuration() + newValue.getDuration())
                                .build()
                        , Materialized.<PadsAggregateKey, PadsAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR)
                                .withKeySerde(AppSerdes.<PadsAggregateKey>Key())
                                .withValueSerde(AppSerdes.<PadsAggregateValue>Value())
                );

        KT_0.toStream().print(Printed.<PadsAggregateKey, PadsAggregateValue>toSysOut().withLabel("HOUR"));
        KT_0.toStream()
                .to(AppConfig.NOBILL_PADS_RECORD_HOUR_TOPIC, Produced.with(AppSerdes.<PadsAggregateKey>Key(), AppSerdes.<PadsAggregateValue>Value()));
    }

    //TODO
    int test(Integer prev, Integer newUnit) {
        if (prev != null && !prev.equals(newUnit))
            logger.info("DIFFERENT QUOTA-UNIT FROM {} to {}", prev, newUnit);
        return newUnit;
    }

    @Override
    public void dayAggregate(StreamsBuilder streamsBuilder, NobillSerdes AppSerdes) {
        KTable<PadsAggregateKey, PadsAggregateValue> KT_0 = streamsBuilder
                .table(AppConfig.NOBILL_PADS_RECORD_HOUR_TOPIC, Consumed.with(AppSerdes.<PadsAggregateKey>Key(), AppSerdes.<PadsAggregateValue>Value())
                        , Materialized.<PadsAggregateKey, PadsAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_HOUR_CONSUMER)
                                .withKeySerde(AppSerdes.<PadsAggregateKey>Key())
                                .withValueSerde(AppSerdes.<PadsAggregateValue>Value()));

        KTable<PadsAggregateKey, PadsAggregateValue> KT_1 = KT_0
                .groupBy((key, value) -> KeyValue.pair(RecordBuilder.PadsAggregateKey(key), value), Grouped.with(AppSerdes.<PadsAggregateKey>Key(), AppSerdes.<PadsAggregateValue>Value()))
                .aggregate(
                        () -> PadsAggregateValue.newBuilder().setPadsCount(0L).setCost(0.0).setUsedQuota(0L).setChargedQuota(0L).setDataVolumeSent(0L).setDataVolumeReceived(0L).setDuration(0L).build(),
                        (k, newValue, agg) -> PadsAggregateValue.newBuilder()
                                .setPadsCount(agg.getPadsCount() + newValue.getPadsCount())
                                .setCost(agg.getCost() + newValue.getCost())
                                .setUsedQuota(agg.getUsedQuota() + newValue.getUsedQuota())
                                .setChargedQuota(agg.getChargedQuota() + newValue.getChargedQuota())
                                .setQuotaUnit(test(agg.getQuotaUnit(), newValue.getQuotaUnit()))
                                .setDataVolumeSent(agg.getDataVolumeSent() + newValue.getDataVolumeSent())
                                .setDataVolumeReceived(agg.getDataVolumeReceived() + newValue.getDataVolumeReceived())
                                .setDuration(agg.getDuration() + newValue.getDuration())
                                .build()
                        , (k, newValue, agg) -> PadsAggregateValue.newBuilder()
                                .setPadsCount(agg.getPadsCount() - newValue.getPadsCount())
                                .setCost(agg.getCost() - newValue.getCost())
                                .setUsedQuota(agg.getUsedQuota() - newValue.getUsedQuota())
                                .setChargedQuota(agg.getChargedQuota() - newValue.getChargedQuota())
                                .setQuotaUnit(test(agg.getQuotaUnit(), newValue.getQuotaUnit()))
                                .setDataVolumeSent(agg.getDataVolumeSent() - newValue.getDataVolumeSent())
                                .setDataVolumeReceived(agg.getDataVolumeReceived() - newValue.getDataVolumeReceived())
                                .setDuration(agg.getDuration() - newValue.getDuration())
                                .build()
                        , Materialized.<PadsAggregateKey, PadsAggregateValue, KeyValueStore<Bytes, byte[]>>as(AppConfig.STATE_STORE_DAY)
                                .withKeySerde(AppSerdes.<PadsAggregateKey>Key())
                                .withValueSerde(AppSerdes.<PadsAggregateValue>Value())
                );
        KT_1.toStream().print(Printed.<PadsAggregateKey, PadsAggregateValue>toSysOut().withLabel("DAY"));
        KT_1.toStream().to(AppConfig.NOBILL_PADS_RECORD_DAY_TOPIC, Produced.with(AppSerdes.<PadsAggregateKey>Key(), AppSerdes.<PadsAggregateValue>Value()));
    }
}
