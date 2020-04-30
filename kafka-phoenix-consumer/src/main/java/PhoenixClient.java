import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;

public class PhoenixClient {

    private Logger logger = LoggerFactory.getLogger(PhoenixClient.class.getName());

    private final Connection connection;
    private PreparedStatement ps;
    private PreparedStatement archivePs;
    private PreparedStatement deletePs;
    private TableQueue queue;
    private TableQueue.Table table;
    private final Properties properties;

    public PhoenixClient(Properties properties) {
        this.properties = properties;
        this.queue = new TableQueue();
        try {
            logger.info("Configure Phoenix Connection");
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            final Properties props = new Properties();
            props.setProperty("hbase.rpc.timeout", "600000");
            props.setProperty("hbase.client.scanner.max.result.size", "10000000");
            props.setProperty("hbase.client.scanner.timeout.period", "6000000");
            props.setProperty("phoenix.query.timeoutMs", "6000000");
            this.connection = DriverManager.getConnection(String.format("jdbc:phoenix:%s:/hbase-unsecure", properties.getProperty(ConfigParser.HBASE_ZOOKEEPER_QUORUM_CONFIG_CONFIG)), props);
            this.connection.setAutoCommit(false);
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("PhoenixClient", e);
            throw new RuntimeException(e);
        }
    }

    public void preparedStatementIfNotNull(TableQueue.Table table) throws SQLException {
        if (ps != null)
            return;
        String timeField = properties.getProperty(ConfigParser.PRIMARY_KEYS_CONFIG).split(",")[0];
        ps = connection.prepareStatement(String.format("SELECT * FROM %s WHERE %s >= ? AND  %s < ? ", table.getTableName(), timeField, timeField));
    }

    public ResultSet nextResultSet() throws SQLException {
        if (queue.isEmpty())
            fillQueue();

        table = queue.poll();

        if (table != null) {
            preparedStatementIfNotNull(table);
            logger.info("Get data from {} to {}",
                    Instant.ofEpochMilli(table.getStartTime().getTime()).atZone(ZoneId.of("Europe/Oslo")),
                    Instant.ofEpochMilli(table.getEndTime().getTime()).atZone(ZoneId.of("Europe/Oslo")));
            ps.setDate(1, table.getStartTime());
            ps.setDate(2, table.getEndTime());

            table.setProducerStartTime(new Date(Instant.now().toEpochMilli()));
            return ps.executeQuery();
        }
        logger.info("Queue is empty");
        queue.setDone(true);
        return null;
    }

    private void fillQueue() throws SQLException {
        String tableName = properties.getProperty(ConfigParser.TABLE_NAME_CONFIG);
        logger.info("QUERY: SELECT * FROM KAFKA.PRODUCER_QUEUE WHERE TABLE_NAME = '{}' AND STATUS IS NULL ORDER BY END_TIME DESC LIMIT 25", tableName);
        queue.setDone(false);
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM KAFKA.PRODUCER_QUEUE WHERE TABLE_NAME = ? AND STATUS IS NULL ORDER BY END_TIME DESC LIMIT 25");
        PreparedStatement updateStatus = connection.prepareStatement("UPSERT INTO KAFKA.PRODUCER_QUEUE (TABLE_NAME, START_TIME, END_TIME, STATUS) VALUES (?,?,?,?)");
        ps.setString(1, properties.getProperty(ConfigParser.TABLE_NAME_CONFIG));
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Date from = rs.getDate("START_TIME");
            Date to = rs.getDate("END_TIME");
            TableQueue.Table table = new TableQueue.Table(from, to, tableName);
            logger.debug("ADD {} to queue", table);
            queue.add(table);

            updateStatus.setString(1, tableName);
            updateStatus.setDate(2, from);
            updateStatus.setDate(3, to);
            updateStatus.setString(4, "SYNCING");
            updateStatus.addBatch();
        }
        updateStatus.executeBatch();
        connection.commit();
        updateStatus.clearBatch();

        logger.info("Queue-size: {}", queue.size());
    }

    public void archive() throws SQLException {
        table.end();
        if (archivePs == null)
            archivePs = connection.prepareStatement("UPSERT INTO KAFKA.PRODUCER_QUEUE_ARCHIVE (TABLE_NAME, START_TIME, END_TIME, PRODUCER_START_TIME, PRODUCER_END_TIME,TOTAL_TIME) VALUES (?,?,?,?,?,?)");
        archivePs.setString(1, table.getTableName());
        archivePs.setDate(2, table.getStartTime());
        archivePs.setDate(3, table.getEndTime());
        archivePs.setDate(4, table.getProducerStartTime());
        archivePs.setDate(5, table.getProducerEndTime());
        archivePs.setLong(6, table.getTotalTime());

        archivePs.executeUpdate();

        if (deletePs == null)
            deletePs = connection.prepareStatement("DELETE FROM KAFKA.PRODUCER_QUEUE WHERE TABLE_NAME = ? AND START_TIME = ? AND END_TIME = ? ");
        deletePs.setString(1, table.getTableName());
        deletePs.setDate(2, table.getStartTime());
        deletePs.setDate(3, table.getEndTime());

        deletePs.executeUpdate();
        connection.commit();
    }

    public boolean isDone() {
        return queue.isDone();
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error("", e);
        }
    }

    public ResultSet customResultSet(String query) throws SQLException {
        ps = connection.prepareStatement(query);
        queue.setDone(true);
        return ps.executeQuery();
    }
}
