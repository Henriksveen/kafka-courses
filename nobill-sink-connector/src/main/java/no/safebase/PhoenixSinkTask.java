package no.safebase;

import lombok.extern.slf4j.Slf4j;
import no.safebase.utils.VersionUtil;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class PhoenixSinkTask extends SinkTask {

    private PhoenixSinkConnectorConfig config;
    private PhoenixClient phoenixClient;
    private Map<String, RecordBuffer> globalBuffer = new HashMap<>();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new PhoenixSinkConnectorConfig(props);
        this.phoenixClient = new PhoenixClient(config);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.info("Fetch {} records", records.size());

        Map<String, List<SinkRecord>> groupedByTopic = records.stream()
                .collect(Collectors.groupingBy(ConnectRecord::topic));

        Map<String, RecordBuffer> newBuffers = groupedByTopic.entrySet().parallelStream()
                .collect(Collectors.toMap(Map.Entry::getKey, es -> new RecordBuffer(es.getValue())));

        newBuffers.forEach(this::mergeToGlobalBuffer);
    }

    private void mergeToGlobalBuffer(String topicName, RecordBuffer newBuffer) {
        RecordBuffer buffer = globalBuffer.get(topicName);
        if (buffer != null)
            buffer.merge(newBuffer);
        else
            globalBuffer.put(topicName, newBuffer);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        log.info("Flush and execute");
        globalBuffer.forEach((topicName, recordBuffer) -> phoenixClient.execute(topicName, recordBuffer));
        globalBuffer.clear();
    }

    @Override
    public void stop() {
        phoenixClient.close();
    }
}
