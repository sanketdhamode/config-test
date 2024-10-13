import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetFileWriter;

import java.io.File;
import java.nio.file.Paths;
import org.apache.hadoop.fs.Path;

public class SQLToParquet {

    public static void main(String[] args) {
        String jdbcUrl = "jdbc:sqlserver://yourserver.database.windows.net:1433;database=yourdatabase";
        String username = "yourusername";
        String password = "yourpassword";
        String parquetFilePath = "output.parquet";
        String paymentDate = "2023-12-31";

        String query = "SELECT * FROM your_table WHERE payment_date = ?";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(query)) {
             
            stmt.setString(1, paymentDate);
            ResultSet rs = stmt.executeQuery();

            // Define your Avro schema based on the SQL table structure
            Schema schema = new Schema.Parser().parse(new File("schema.avsc"));

            // Configure Parquet writer
            Path path = new Path(parquetFilePath);
            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build();

            // Iterate over the ResultSet and write each row as a record in the Parquet file
            while (rs.next()) {
                GenericRecord record = new GenericData.Record(schema);
                // Assuming you know the column names, map them to the schema
                record.put("column1", rs.getString("column1"));
                record.put("column2", rs.getInt("column2"));
                // Continue for all columns...
                
                writer.write(record);
            }

            writer.close();
            System.out.println("Data successfully written to Parquet file.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
