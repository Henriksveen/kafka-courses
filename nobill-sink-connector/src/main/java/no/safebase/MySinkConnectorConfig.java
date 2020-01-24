package no.safebase;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MySinkConnectorConfig extends AbstractConfig {

    private static final String TOPIC_CONFIG = "topic";
    private static final String TOPIC_DOC = "Topic to write to";

    public MySinkConnectorConfig(ConfigDef config, Map<String, String> pasedConfig) {
        super(config, pasedConfig);
    }

    public MySinkConnectorConfig(Map<String, String> pasedConfig) {
        super(conf(), pasedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DOC);
    }
}
