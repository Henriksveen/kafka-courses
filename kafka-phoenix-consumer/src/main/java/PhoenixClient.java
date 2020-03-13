import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
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
            props.setProperty("hbase.rpc.timeout", "150000");
            props.setProperty("hbase.client.scanner.max.result.size", "10000000");
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
        ps = connection.prepareStatement(String.format("SELECT * FROM %s WHERE MEASTIME >= ? AND  MEASTIME <= ? ", table.getTableName()));
    }

    public ResultSet nextResultSet() throws SQLException {
        if (queue.isEmpty())
            fillQueue();

        table = queue.poll();

        if (table != null) {
            preparedStatementIfNotNull(table);
            logger.info("Get data from {} to {}", table.getStartTime(), table.getEndTime());
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
        logger.info("QUERY: SELECT * FROM KAFKA.PRODUCER_QUEUE WHERE TABLE_NAME = '{}' ORDER BY END_TIME DESC LIMIT 25", tableName);
        queue.setDone(false);
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM KAFKA.PRODUCER_QUEUE WHERE TABLE_NAME = ? ORDER BY END_TIME DESC LIMIT 25");
        ps.setString(1, properties.getProperty(ConfigParser.TABLE_NAME_CONFIG));
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Date from = rs.getDate("START_TIME");
            Date to = rs.getDate("END_TIME");
            TableQueue.Table table = new TableQueue.Table(from, to, tableName);
            logger.debug("ADD {} to queue", table);
            queue.add(table);
        }
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
}
