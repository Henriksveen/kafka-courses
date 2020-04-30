import lombok.extern.slf4j.Slf4j;
import org.apache.phoenix.query.QueryServices;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class CreateTable {

    String driverPath = "/home/henriksveen/drivers/phoenix-5.0.0-HBase-2.0-client.jar";
    String tablename = "ERICSSON_4G_CELL";

    Map<String, String> colMap = new HashMap<>();
    Set<String> cols = new HashSet<>(Collections.singletonList("MTS"));

    private Connection connection;
    private PreparedStatement checkCols;

    public static void main(String[] args) throws Exception {
        new CreateTable().run();
    }

    private void run() throws Exception {
        connect();
        meta();
        convert();

        String create = "CREATE TABLE COPY.PM_4G_ERICSSON_CELL_HOUR (\n" +
                "\tMTS date not null,\n" +
                "\tNEUN varchar,\n" +
                "\tCELL_ID bigint not null,\n" +
                counters() +
                "CONSTRAINT pk PRIMARY KEY (MTS row_timestamp,NEUN,CELL_ID))\n" +
                "COMPRESSION='SNAPPY',SALT_BUCKETS=6, UPDATE_CACHE_FREQUENCY=900000, COLUMN_ENCODED_BYTES=3,BLOOMFILTER='ROW'\n";

        String select = "SELECT MTS,NEUN,CELL_ID," + select() + " FROM PM_4G_ERICSSON_CELL_HOUR where MTS = to_date('2020-01-23 00:00:00+01:00')";
//        System.out.println(create);
//        System.out.println(select);
    }


    private void meta() {
        try {
            if (checkCols == null)
                checkCols = connection.prepareStatement("select column_name,column_qualifier from SYSTEM.CATALOG where table_schem=? and table_name=? and column_name is not null order by column_qualifier");
            checkCols.setString(1, "META");
            checkCols.setString(2, tablename);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void convert() {
        try (ResultSet result = checkCols.executeQuery()) {
            while (result.next()) {
                String col = result.getString(1);
                if (cols.contains(col)) // cols to ignore
                    continue;
                byte[] qualifier = result.getBytes(2);
                if (qualifier != null)
                    colMap.put(col, Converter.bytesToHex(qualifier));
                else {
                    colMap.put(col, col); // SHOULD NEVER HAPPEN!!
                    throw new RuntimeException(col + " has no column_qualifier in " + tablename);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void connect() {
        log.debug("Configuring Phoenix Connection");
        try {
            URLClassLoader clsLoader = URLClassLoader.newInstance(new URL[]{new URL("file:" + driverPath)});

            Class<?> phoenixDriverClass = clsLoader.loadClass("org.apache.phoenix.jdbc.PhoenixDriver");
            Driver driver = (Driver) phoenixDriverClass.newInstance();

            final Properties props = new Properties();
            props.put("hbase.zookeeper.quorum", "no001smmast01.ddc.teliasonera.net");
            props.put("hbase.rpc.timeout", "600000");
            props.setProperty(QueryServices.MAX_MUTATION_SIZE_BYTES_ATTRIB, String.valueOf(Integer.MAX_VALUE)); // Todo: config based - maybe use other driver
            props.setProperty(QueryServices.MUTATE_BATCH_SIZE_BYTES_ATTRIB, String.valueOf(Integer.MAX_VALUE)); // Todo: config based - maybe use other driver

            log.debug("Starting to connect " + props);
            connection = driver.connect("jdbc:phoenix:no001smmast01.ddc.teliasonera.net:/hbase-unsecure", props);
            log.debug(connection.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String select() throws Exception {
        StringBuilder builder = new StringBuilder();
        URL resource = getClass().getClassLoader().getResource("counters.txt");
        assert resource != null;
        AtomicInteger i = new AtomicInteger(0);
        Files.lines(Paths.get(resource.toURI())).forEach(s -> builder.append("\"").append(s).append("\","));
        String sel = builder.toString();
        sel = sel.substring(0, sel.length() - 1);
        return sel;
    }

    private String counters() throws Exception {
        StringBuilder builder = new StringBuilder();
        URL resource = getClass().getClassLoader().getResource("counters.txt");
        assert resource != null;
        AtomicInteger i = new AtomicInteger(0);
        Files.lines(Paths.get(resource.toURI())).forEach(s -> builder.append("\"").append(s).append("\"").append(" bigint,").append(i.getAndIncrement() % 500 == 0 ? "\n" : ""));
        return builder.toString();
    }
}
