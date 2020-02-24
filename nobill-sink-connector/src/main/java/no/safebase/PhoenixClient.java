package no.safebase;

import lombok.extern.slf4j.Slf4j;
import no.safebase.utils.Counter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PhoenixClient {

    private PhoenixSinkConnectorConfig config;

    private final Connection connection;
    private final Map<String, PreparedStatement> prepared = new HashMap<>();
    private final int batchSize;
    private final List<String> dateFields;

    public PhoenixClient(final PhoenixSinkConnectorConfig config) {
        this.config = config;
        this.batchSize = config.getBatchSize();
        this.dateFields = Arrays.asList(config.getDateField().split(","));
        try {
            log.info("Configure Phoenix Connection");
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            final Properties props = new Properties();
            props.put("hbase.zookeeper.quorum", config.getHbaseZookeeperQuorum());
            props.put("hbase.rpc.timeout", "600000");
            this.connection = DriverManager.getConnection(String.format("jdbc:phoenix:%s:/hbase-unsecure", config.getHbaseZookeeperQuorum()), props);
            this.connection.setAutoCommit(false);
        } catch (SQLException | ClassNotFoundException e) {
            log.error("PhoenixClient", e);
            throw new RuntimeException(e);
        }
    }

    public void execute(String topic, RecordBuffer buffer) {
        long l = System.currentTimeMillis();
        Counter<String> counter = new Counter<>();
        try {
            for (SinkRecord record : buffer.getRecords()) {
                Schema keySchema = record.keySchema();
                Schema valueSchema = record.valueSchema();
                Struct keyStruct = (Struct) record.key();
                Struct valueStruct = (Struct) record.value();

                String query = formUpsert(keySchema, valueSchema, valueStruct, topic.toUpperCase().replace('-', '_'));
                PreparedStatement ps = getStatement(query);

                formStatement(ps, keySchema, valueSchema, keyStruct, valueStruct);
                ps.addBatch();
                if (counter.count(topic) % batchSize == 0) {
                    ps.executeBatch();
                    connection.commit();
                    ps.clearBatch();
                }
            }
            for (PreparedStatement stmt : prepared.values()) {
                stmt.executeBatch();
                connection.commit();
                stmt.clearBatch();
            }
        } catch (SQLException e) {
            log.error("PhoenixClient.execute()", e);
            throw new RuntimeException(e);
        }
        log.info("Data insert of total {} records took {}ms", counter.totalSum(), System.currentTimeMillis() - l);
    }

    private PreparedStatement getStatement(String query) throws SQLException {
        PreparedStatement stmnt = prepared.get(query);
        if (stmnt == null) {
            stmnt = connection.prepareStatement(query);
            prepared.put(query, stmnt);
        }
        return stmnt;
    }

    public PreparedStatement formStatement(PreparedStatement ps, Schema keySchema, Schema valueSchema, Struct keyStruct, Struct valueStruct) {
        try {
            AtomicInteger paramIndex = new AtomicInteger(1);
            setParams(ps, keySchema, keyStruct, paramIndex);
            setParams(ps, valueSchema, valueStruct, paramIndex);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return ps;
    }

    private void setParams(PreparedStatement ps, Schema schema, Struct struct, AtomicInteger paramIndex) throws SQLException {
        for (Field f : schema.fields()) {
            Object value = struct.get(f);
            Schema.Type type = f.schema().type();
            switch (type) {
                case STRING:
                    ps.setString(paramIndex.getAndIncrement(), (String) value);
                    break;
                case BOOLEAN:
                    ps.setBoolean(paramIndex.getAndIncrement(), (boolean) value);
                    break;
                case FLOAT32:
                case FLOAT64:
                    ps.setDouble(paramIndex.getAndIncrement(), (double) value);
                    break;
                case INT8:
                case INT16:
                case INT32:
                    ps.setInt(paramIndex.getAndIncrement(), (int) value);
                    break;
                case INT64:
                    if (dateFields.contains(f.name()))
                        ps.setDate(paramIndex.getAndIncrement(), new Date((long) value));
                    else
                        ps.setLong(paramIndex.getAndIncrement(), (long) value);
                    break;
            }
        }
    }

    public String formUpsert(Schema keySchema, Schema valueSchema, Struct valueStruct, String tableName) {
        StringBuilder query = new StringBuilder("upsert into \"" + config.getSchema() + "\".\"" + tableName + "\"(");
        StringBuilder query_part2 = new StringBuilder(") values (");
        keySchema.fields().forEach(f -> {
            query.append(f.name()).append(","); // for now, we do not support nullFields in key
//                query.append("\"").append(f.name()).append("\","); // for now, we do not support nullFields in key
            query_part2.append("?,");
        });
        valueSchema.fields().forEach(f -> {
            Object value = valueStruct.get(f); // to avoid to setNull in prepared statement
            if (value != null) {
                query.append(f.name()).append(",");
//                query.append("\"").append(f.name()).append("\",");
                query_part2.append("?,");
            }
        });
        query.deleteCharAt(query.lastIndexOf(","));
        query_part2.deleteCharAt(query_part2.lastIndexOf(","));
        query.append(query_part2).append(")");
        return query.toString();
    }

    public void close() {
        try {
            if (connection != null) {
                for (PreparedStatement stmt : prepared.values()) {
                    try {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    } catch (SQLException e) {
                        log.error("PhoenixClient.close()", e);
                        throw new RuntimeException(e);
                    }
                }
                connection.commit();
                connection.close();
            }
        } catch (SQLException e) {
            log.error("PhoenixClient.close()", e);
        }
    }
}

//    create table NOBILL.NOBILL_CALL_RECORD_HOUR (
//        period_start date not null,
//        call_type varchar  not null,
//        termination_reason varchar  not null,
//        rate_plan_name varchar not null,
//        call_count bigint,
//        cost double,
//        duration bigint,
//        CONSTRAINT pk PRIMARY KEY (period_start row_timestamp,call_type,termination_reason,rate_plan_name))
//        COMPRESSION='SNAPPY',SALT_BUCKETS=4, UPDATE_CACHE_FREQUENCY=900000, COLUMN_ENCODED_BYTES=3,BLOOMFILTER='ROW';


//   create table NOBILL.NOBILL_PADS_RECORD_HOUR (
//       period_start date not null,
//       call_type integer  not null,
//       termination_reason integer  not null,
//       rate_plan_name varchar not null,
//       subscription_type_name varchar not null,
//       sgsn_address varchar not null,
//       apn varchar not null,
//       pads_count bigint,
//       cost double,
//       duration bigint,
//       data_volume_sent bigint,
//       data_volume_received bigint,
//       used_quota bigint,
//       charged_quota bigint,
//       quota_unit integer,
//       CONSTRAINT pk PRIMARY KEY (period_start row_timestamp,call_type,termination_reason,rate_plan_name,subscription_type_name,sgsn_address,apn))
//       COMPRESSION='SNAPPY',SALT_BUCKETS=4, UPDATE_CACHE_FREQUENCY=900000, COLUMN_ENCODED_BYTES=3,BLOOMFILTER='ROW';