package no.safebase;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class MySinkTast extends SinkTask {
    @Override
    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {

    }

    @Override
    public void put(Collection<SinkRecord> records) {

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        return super.preCommit(currentOffsets);
    }

    @Override
    public void stop() {

    }
}
