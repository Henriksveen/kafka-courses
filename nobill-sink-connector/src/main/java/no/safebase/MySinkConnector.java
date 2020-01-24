package no.safebase;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.List;
import java.util.Map;

public class MySinkConnector extends SinkConnector {
    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return null;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public String version() {
        return null;
    }
}
