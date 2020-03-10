import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class PhoenixClient {

    private Logger logger = LoggerFactory.getLogger(PhoenixClient.class.getName());

    private final Connection connection;
    private PreparedStatement ps;

    public PhoenixClient(String quorum) {
        try {
            logger.info("Configure Phoenix Connection");

            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            final Properties props = new Properties();
            props.setProperty("hbase.rpc.timeout", "150000");
            props.setProperty("hbase.client.scanner.max.result.size", "10000000");
            this.connection = DriverManager.getConnection(String.format("jdbc:phoenix:%s:/hbase-unsecure", quorum), props);
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("PhoenixClient", e);
            throw new RuntimeException(e);
        }
    }


    public void prepareStatement(String select) throws SQLException {
        this.ps = connection.prepareStatement(select);
    }

    public void setDate(Date date) throws SQLException {
        ps.setDate(1, date);
    }

    public ResultSet executeQuery() throws SQLException {
        return ps.executeQuery();
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error("", e);
        }
    }
}
