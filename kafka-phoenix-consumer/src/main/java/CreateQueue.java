import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Properties;

public class CreateQueue {
    private Logger logger = LoggerFactory.getLogger(CreateQueue.class.getName());

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        new CreateQueue().run();
    }

    private CreateQueue() {
    }

    private void run() {
        try {
            Connection connection = getConnection();
            PreparedStatement ps = connection.prepareStatement("UPSERT INTO KAFKA.PRODUCER_QUEUE (TABLE_NAME, START_TIME, END_TIME) VALUES (?,?,?)");
            ZonedDateTime from = Instant.ofEpochMilli(toEpoch("2018-10-01 00:00:00")).atZone(ZoneId.of("Europe/Oslo"));
            ZonedDateTime max = Instant.ofEpochMilli(toEpoch("2020-04-27 00:00:00")).atZone(ZoneId.of("Europe/Oslo"));

            int interval = 1;

            while (from.isBefore(max)) {
                ZonedDateTime to = from.plusDays(interval);

                ps.setString(1, "FEED.DEVICE_RATS_BANDS_DAY");
                ps.setDate(2, new Date(from.toEpochSecond() * 1000));
                ps.setDate(3, new Date(to.toEpochSecond() * 1000));
                ps.addBatch();

                from = to;
            }

            ps.executeBatch();
            connection.commit();
            ps.clearBatch();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static long toEpoch(String time) {
        try {
            ZonedDateTime zonedDateTime = ZonedDateTime.of(LocalDateTime.parse(time, formatter), ZoneId.of("Europe/Oslo"));
            return Instant.from(zonedDateTime).getEpochSecond() * 1000;
        } catch (DateTimeParseException e) {
            throw new RuntimeException("Failed to parse date:", e);
        } catch (NullPointerException e) {
            throw new RuntimeException("Value is null:", e);
        }
    }

    private Connection getConnection() {
        Connection connection;
        try {
            logger.info("Configure Phoenix Connection");
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            final Properties props = new Properties();
            props.setProperty("hbase.rpc.timeout", "150000");
            props.setProperty("hbase.client.scanner.max.result.size", "10000000");
            connection = DriverManager.getConnection("jdbc:phoenix:tp001smrep01.ddc.teliasonera.net:/hbase-unsecure", props);
            connection.setAutoCommit(false);
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("PhoenixClient", e);
            throw new RuntimeException(e);
        }
        return connection;
    }
}
