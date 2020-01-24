package no.safebase;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MySinkConnectorConfig extends AbstractConfig {

    public MySinkConnectorConfig(ConfigDef config, Map<String, String> pasedConfig) {
        super(config, pasedConfig);
    }
}
