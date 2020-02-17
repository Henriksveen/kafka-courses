package no.safebase;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordBuffer {

    private Map<String, SinkRecord> buffer = new HashMap<>();

    public RecordBuffer(List<SinkRecord> records) {
        records.forEach(record -> buffer.put(extractKey(record), record));
    }

    private String extractKey(SinkRecord record) {
        Struct key = (Struct) record.key();
        Schema schema = record.keySchema();

        StringBuilder builder = new StringBuilder(record.topic());

        schema.fields().forEach(field -> builder.append(key.get(field)));
        return builder.toString();
    }

    public Collection<SinkRecord> getRecords() {
        return buffer.values();
    }

    public void merge(RecordBuffer other) {
        other.buffer.forEach(this.buffer::put);
    }
}
