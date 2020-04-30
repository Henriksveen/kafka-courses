import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class SchemaMapping {

    private static Logger log = LoggerFactory.getLogger(SchemaMapping.class.getName());

    public static Schema keySchema(String schemaName, ResultSetMetaData metaData, List<String> primaryKeys) throws SQLException {
        SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(schemaName).namespace("no.safebase").fields();

        for (int columnNumber = 1; columnNumber <= metaData.getColumnCount(); ++columnNumber) {
            String columnName = metaData.getColumnName(columnNumber);
            if (primaryKeys.stream().noneMatch(columnName::equalsIgnoreCase))
                continue; // Ignore non-primaryKeys

            int columnType = metaData.getColumnType(columnNumber);
            String columnTypeName = metaData.getColumnTypeName(columnNumber);

            addColumn(builder, columnName, columnType, columnTypeName);
        }

        return builder.endRecord();
    }

    public static Schema valueSchema(String schemaName, ResultSetMetaData metaData, List<String> primaryKeys) throws SQLException {
        SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(schemaName).namespace("no.safebase").fields();

        for (int columnNumber = 1; columnNumber <= metaData.getColumnCount(); ++columnNumber) {
            String columnName = metaData.getColumnName(columnNumber);
            if (primaryKeys.stream().anyMatch(columnName::equalsIgnoreCase))
                continue; // Ignore primaryKeys

            int columnType = metaData.getColumnType(columnNumber);
            String columnTypeName = metaData.getColumnTypeName(columnNumber);
            addColumn(builder, columnName, columnType, columnTypeName);
        }
        return builder.endRecord();
    }

    private static void addColumn(SchemaBuilder.FieldAssembler<Schema> builder, String columnName, int columnType, String columnTypeName) {
        switch (columnType) {
            case Types.VARCHAR:
                builder.optionalString(columnName);
                break;
            case Types.BOOLEAN:
                builder.optionalBoolean(columnName);
                break;
            case Types.DOUBLE:
                builder.optionalDouble(columnName);
                break;
            case Types.INTEGER:
                builder.optionalInt(columnName);
                break;
            case Types.BIGINT:
            case Types.DATE:
                builder.optionalLong(columnName);
                break;
            default:
                log.error("JDBC type {} ({}) not currently supported", columnType, columnTypeName);
                throw new RuntimeException();
        }
    }
}
