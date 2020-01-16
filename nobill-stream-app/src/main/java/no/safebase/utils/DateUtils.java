package no.safebase.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateUtils {

    private static SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");

    public static long toEpoch(String time) {
        try {
            return df.parse(time).getTime();
        } catch (ParseException e) {
            throw new RuntimeException("Failed to parse date:", e);
        } catch (NullPointerException e) {
            throw new RuntimeException("Value is null:", e);
        }
    }
}
