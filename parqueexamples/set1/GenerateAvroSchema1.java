import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class GenerateAvroSchema {

    public static void main(String[] args) {
        String jdbcUrl = "jdbc:sqlserver://yourserver.database.windows.net:1433;database=yourdatabase";
        String username = "yourusername";
        String password = "yourpassword";
        String tableName = "your_table";
        String schemaFilePath = "schema.avsc";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, tableName, null);

            // Start building the schema
            SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record("TableRecord");
            SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String columnType = columns.getString("TYPE_NAME");

                // Map SQL types to Avro types
                switch (columnType.toUpperCase()) {
                    case "VARCHAR":
                    case "CHAR":
                    case "TEXT":
                        fieldAssembler.name(columnName).type().nullable().stringType().noDefault();
                        break;
                    case "INT":
                    case "INTEGER":
                        fieldAssembler.name(columnName).type().nullable().intType().noDefault();
                        break;
                    case "BIGINT":
                        fieldAssembler.name(columnName).type().nullable().longType().noDefault();
                        break;
                    case "FLOAT":
                    case "REAL":
                        fieldAssembler.name(columnName).type().nullable().floatType().noDefault();
                        break;
                    case "DOUBLE":
                        fieldAssembler.name(columnName).type().nullable().doubleType().noDefault();
                        break;
                    case "NUMERIC":
                    case "DECIMAL":
                        // Use stringType to preserve precision or use doubleType if slight precision loss is acceptable
                        fieldAssembler.name(columnName).type().nullable().stringType().noDefault();
                        break;
                    case "BIT":
                    case "BOOLEAN":
                        fieldAssembler.name(columnName).type().nullable().booleanType().noDefault();
                        break;
                    case "DATE":
                    case "TIMESTAMP":
                        fieldAssembler.name(columnName).type().nullable().stringType().noDefault();
                        break;
                    default:
                        fieldAssembler.name(columnName).type().nullable().stringType().noDefault();
                        break;
                }
            }

            Schema schema = fieldAssembler.endRecord();

            // Save schema to file
            try (FileWriter fileWriter = new FileWriter(new File(schemaFilePath))) {
                fileWriter.write(schema.toString(true));
            }
            System.out.println("Schema successfully generated and saved to " + schemaFilePath);

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
