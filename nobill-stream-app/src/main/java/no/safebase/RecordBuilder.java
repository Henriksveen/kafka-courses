package no.safebase;

import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallRecordValue;
import no.safebase.utils.DateUtils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class RecordBuilder {

    public static ZoneId zoneId = ZoneId.of("Europe/Oslo");

    public static CallAggregateKey CallAggregateKey(CallRecordValue value) {
        long epoch = DateUtils.toEpoch(value.getStarttime());
        return CallAggregateKey.newBuilder()
                .setAggTime(Instant.ofEpochMilli(epoch).atZone(zoneId).truncatedTo(ChronoUnit.HOURS).toEpochSecond() * 1000L)
                .setCallType(value.getCalltype())
                .setTerminationReason(value.getTerminationreason())
                .setRatePlanName(value.getRateplanname().trim())
                .build();
    }

    public static CallAggregateKey CallAggregateKey(CallAggregateKey key) {
        return CallAggregateKey.newBuilder(key)
                .setAggTime(Instant.ofEpochMilli(key.getAggTime()).atZone(zoneId).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000L)
                .build();
    }
}
