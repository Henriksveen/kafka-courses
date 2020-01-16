package no.safebase.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.*;

class DateUtilsTest {

    @Test
    @DisplayName("Convert dateString to epoch second with ZoneId=Europe/Oslo")
    void testToEpoch() {
        String dateString = "20191031144047";

        long convertedDateString = DateUtils.toEpoch(dateString);
        long actualWithZoneId = Instant.ofEpochMilli(convertedDateString).atZone(ZoneId.of("Europe/Oslo")).toEpochSecond();

        assertEquals(1572529247L, actualWithZoneId);
    }

    @Test
    @DisplayName("Should throw RuntimeException with message - input null")
    void testToEpochWithNull() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> DateUtils.toEpoch(null));
        assertEquals("Value is null:", exception.getMessage());
    }
    @Test
    @DisplayName("Should throw RuntimeException with message - wrong input")
    void testToEpochWithWrongInput() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> DateUtils.toEpoch("12345"));
        assertEquals("Failed to parse date:", exception.getMessage());
    }
}