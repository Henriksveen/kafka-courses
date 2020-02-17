package no.safebase;

import no.safebase.validators.BatchSizeValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class PhoenixSinkConnectorConfig extends AbstractConfig {

    private static final String TOPIC_CONFIG = "topics";
    private static final String TOPIC_DOC = "Topics to read from";

    private static final String SCHEMA_CONFIG = "schema";
    private static final String SCHEMA_DOC = "The database schema name ";

    private static final String DATE_FIELD_CONFIG = "date.fields";
    private static final String DATE_FIELD_DOC = "The field in the key-schema that will contain the date in epoch ms";

    private static final int BATCH_SIZE_DEFAULT = 10000;
    private static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "Number of records before upsert - default " + BATCH_SIZE_DEFAULT;

    public static final String HBASE_ZOOKEEPER_QUORUM_CONFIG = "hbase.zookeeper.quorum";
    private static final String HBASE_ZOOKEEPER_QUORUM_DOC = "The hbase zookeper quorom.";

    public PhoenixSinkConnectorConfig(Map<String, String> pasedConfig) {
        super(conf(), pasedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC)
                .define(HBASE_ZOOKEEPER_QUORUM_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, HBASE_ZOOKEEPER_QUORUM_DOC)
                .define(SCHEMA_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, SCHEMA_DOC)
                .define(BATCH_SIZE_CONFIG, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, new BatchSizeValidator(), ConfigDef.Importance.LOW, BATCH_SIZE_DOC)
                .define(DATE_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.LOW, DATE_FIELD_DOC);
    }

    public String getHbaseZookeeperQuorum() {
        return this.getString(HBASE_ZOOKEEPER_QUORUM_CONFIG);
    }

    public String getSchema() {
        return this.getString(SCHEMA_CONFIG);
    }

    public int getBatchSize() {
        return this.getInt(BATCH_SIZE_CONFIG);
    }

    public String getDateField() {
        return this.getString(DATE_FIELD_CONFIG);
    }
}
