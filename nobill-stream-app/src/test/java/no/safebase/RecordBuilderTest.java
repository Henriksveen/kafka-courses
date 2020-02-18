package no.safebase;

import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallRecordValue;
import no.safebase.nobill.model.SmsAggregateKey;
import no.safebase.nobill.model.SmsRecordValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.*;

class RecordBuilderTest {

    @Test
    @DisplayName("Test that key selector parse correctly from CallRecordValue")
    void callAggregateKey() {
        CallRecordValue value = CallRecordValue.newBuilder()
                .setInternalreferenceid("123")
                .setCalltype(1)
                .setTerminationreason(1)
                .setRateplanname("Rate Plan Name")
                .setCost(2.0)
                .setStarttime("20200114091500")
                .setDuration(1000L)
                .build();

        CallAggregateKey aggregateKey = RecordBuilder.CallAggregateKey(value);

        assertAll(
                () -> assertEquals(1578988800000L, aggregateKey.getPeriodStart()),
                () -> assertEquals(1, aggregateKey.getCallType()),
                () -> assertEquals("Rate Plan Name", aggregateKey.getRatePlanName()),
                () -> assertEquals(1, aggregateKey.getTerminationReason())
        );
    }

    @Test
    @DisplayName("Test that key selector parse correctly from CallAggregateKey")
    void callAggregateKey2() {

        long hourMillis = 1578988800000L; // 2020:01:14 09:00 GMT+0100
        long dayMillis = 1578956400000L; // 2020:01:14 00:00 GMT+0100

        CallAggregateKey hourKey = CallAggregateKey.newBuilder()
                .setCallType(1)
                .setTerminationReason(1)
                .setRatePlanName("Rate Plan Name")
                .setPeriodStart(hourMillis) // 2020:01:14 09:00 GMT+0100
                .build();

        CallAggregateKey aggregateKey = RecordBuilder.CallAggregateKey(hourKey);

        assertAll(
                () -> assertEquals(dayMillis, aggregateKey.getPeriodStart()),
                () -> assertEquals(1, aggregateKey.getCallType()),
                () -> assertEquals("Rate Plan Name", aggregateKey.getRatePlanName()),
                () -> assertEquals(1, aggregateKey.getTerminationReason())
        );
    }

    @Test
    @DisplayName("Test that keu selector parse correctly from SmsRecordValue")
    void smsAggregateKey() {
        SmsRecordValue value = SmsRecordValue.newBuilder()
                .setInternalreferenceid("123")
                .setCalltype(1)
                .setTerminationreason(1)
                .setSubscriptiontypename("Type")
                .setRateplanname("Rate Plan Name")
                .setCost(2.0)
                .setCreationtime("20200114091500")
                .build();

        SmsAggregateKey aggregateKey = RecordBuilder.SmsAggregateKey(value);

        assertAll(
                () -> assertEquals(1578988800000L, aggregateKey.getPeriodStart()),
                () -> assertEquals(1, aggregateKey.getCallType()),
                () -> assertEquals("Rate Plan Name", aggregateKey.getRatePlanName()),
                () -> assertEquals("Type", aggregateKey.getSubscriptionTypeName())
        );
    }

    @Test
    @DisplayName("Test that keu selector parse correctly from SmsAggregateKey")
    void smsAggregateKey2() {
        long hourMillis = 1578988800000L; // 2020:01:14 09:00 GMT+0100
        long dayMillis = 1578956400000L; // 2020:01:14 00:00 GMT+0100

        SmsAggregateKey hourKey = SmsAggregateKey.newBuilder()
                .setCallType(1)
                .setSubscriptionTypeName("Type")
                .setRatePlanName("Rate Plan Name")
                .setPeriodStart(hourMillis) // 2020:01:14 09:00 GMT+0100
                .build();

        SmsAggregateKey aggregateKey = RecordBuilder.SmsAggregateKey(hourKey);

        assertAll(
                () -> assertEquals(dayMillis, aggregateKey.getPeriodStart()),
                () -> assertEquals(1, aggregateKey.getCallType()),
                () -> assertEquals("Rate Plan Name", aggregateKey.getRatePlanName()),
                () -> assertEquals("Type", aggregateKey.getSubscriptionTypeName())
        );
    }
}