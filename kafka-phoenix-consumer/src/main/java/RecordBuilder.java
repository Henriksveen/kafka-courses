import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;

public class RecordBuilder {

    public GenericRecord extractRecord(Schema schema, ResultSet rs) throws SQLException {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for (Schema.Field f : schema.getFields()) {
            Schema.Type type = f.schema().getTypes().get(f.schema().getTypes().size() -1).getType();
            switch (type) {
                case STRING:
                    builder.set(f, rs.getString(f.name()));
                    break;
                case BOOLEAN:
                    builder.set(f, rs.getBoolean(f.name()));
                    break;
                case DOUBLE:
                    builder.set(f, rs.getDouble(f.name()));
                    break;
                case INT:
                    builder.set(f, rs.getInt(f.name()));
                    break;
                case LONG:
                    builder.set(f, rs.getLong(f.name()));
                    break;
            }
        }
        return builder.build();
    }
}
