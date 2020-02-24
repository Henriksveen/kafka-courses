package no.safebase;

import no.safebase.nobill.model.*;
import no.safebase.utils.DateUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class RecordBuilder {

    public static ZoneId zoneId = ZoneId.of("Europe/Oslo");

    public static CallAggregateKey CallAggregateKey(CallRecordValue value) {
        long epoch = DateUtils.toEpoch(value.getStarttime());
        return CallAggregateKey.newBuilder()
                .setPeriodStart(Instant.ofEpochMilli(epoch).atZone(zoneId).truncatedTo(ChronoUnit.HOURS).toEpochSecond() * 1000L)
                .setCallType(value.getCalltype())
                .setTerminationReason(value.getTerminationreason())
                .setRatePlanName(value.getRateplanname().trim())
                .build();
    }

    public static CallAggregateKey CallAggregateKey(CallAggregateKey key) {
        return CallAggregateKey.newBuilder(key)
                .setPeriodStart(Instant.ofEpochMilli(key.getPeriodStart()).atZone(zoneId).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000L)
                .build();
    }

    public static SmsAggregateKey SmsAggregateKey(SmsRecordValue value) {
        long epoch = DateUtils.toEpoch(value.getTstamp());
        return SmsAggregateKey.newBuilder()
                .setPeriodStart(Instant.ofEpochMilli(epoch).atZone(zoneId).truncatedTo(ChronoUnit.HOURS).toEpochSecond() * 1000L)
                .setCallType(value.getCalltype())
                .setSubscriptionTypeName(value.getSubscriptiontypename())
                .setRatePlanName(value.getRateplanname().trim())
                .build();
    }

    public static SmsAggregateKey SmsAggregateKey(SmsAggregateKey key) {
        return SmsAggregateKey.newBuilder(key)
                .setPeriodStart(Instant.ofEpochMilli(key.getPeriodStart()).atZone(zoneId).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000L)
                .build();
    }

    public static PadsAggregateKey PadsAggregateKey(PadsRecordValue value) {
        long epoch = DateUtils.toEpoch(value.getStarttime());
        return PadsAggregateKey.newBuilder()
                .setPeriodStart(Instant.ofEpochMilli(epoch).atZone(zoneId).truncatedTo(ChronoUnit.HOURS).toEpochSecond() * 1000L)
                .setCallType(value.getCalltype())
                .setTerminationReason(value.getTerminationreason())
                .setSubscriptionTypeName(value.getSubscriptiontypename())
                .setRatePlanName(value.getRateplanname().trim())
                .setApn(value.getApn())
                .setSgsnAddress(value.getSgsnaddress())
                .build();
    }

    public static PadsAggregateKey PadsAggregateKey(PadsAggregateKey key) {
        return PadsAggregateKey.newBuilder(key)
                .setPeriodStart(Instant.ofEpochMilli(key.getPeriodStart()).atZone(zoneId).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000L)
                .build();
    }
}
