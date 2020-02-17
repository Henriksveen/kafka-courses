package no.safebase;

import no.safebase.nobill.model.CallAggregateKey;
import no.safebase.nobill.model.CallRecordValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
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


        Windowed<CallAggregateKey> value = new Windowed<>(hourKey, new TimeWindow(hourMillis, hourMillis * 60 * 60 * 1000));

        CallAggregateKey aggregateKey = RecordBuilder.CallAggregateKey(value.key());

        assertAll(
                () -> assertEquals(dayMillis, aggregateKey.getPeriodStart()),
                () -> assertEquals(1, aggregateKey.getCallType()),
                () -> assertEquals("Rate Plan Name", aggregateKey.getRatePlanName()),
                () -> assertEquals(1, aggregateKey.getTerminationReason())
        );
    }
}