package no.safebase;

import lombok.extern.slf4j.Slf4j;
import no.safebase.utils.VersionUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class PhoenixSinkConnector extends SinkConnector {

    private PhoenixSinkConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        this.config = new PhoenixSinkConnectorConfig(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PhoenixSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(config.originalsStrings());
        }
        return configs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return PhoenixSinkConnectorConfig.conf();
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }
}
