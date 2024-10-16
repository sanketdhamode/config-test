import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Value("#{${tableConfig}}") // Load table configurations
    private Map<String, TableConfig> tableConfig;

    @Bean
    public Job exportDataJob() {
        return jobBuilderFactory.get("exportDataJob")
                .incrementer(new RunIdIncrementer())
                .start(stepRetrieveEventDates())
                .next(stepExportData())
                .build();
    }

    // Step to retrieve multiple event dates
    @Bean
    public Step stepRetrieveEventDates() {
        return stepBuilderFactory.get("stepRetrieveEventDates")
                .tasklet((contribution, chunkContext) -> {
                    JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                    String sql = "SELECT eventDate FROM your_event_table WHERE condition = ?"; // Adjust as needed
                    List<String> eventDates = jdbcTemplate.queryForList(sql, String.class, "your_condition");

                    // Store event dates in execution context
                    chunkContext.getStepContext().getStepExecution().getExecutionContext().put("eventDates", eventDates);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    // Step to export data using the retrieved event dates
    @Bean
    public Step stepExportData() {
        return stepBuilderFactory.get("stepExportData")
                .<Map<String, Object>, Map<String, Object>>chunk(100)
                .reader(tableItemReader())
                .writer(tableItemWriter())
                .build();
    }

    @Bean
    public ItemReader<Map<String, Object>> tableItemReader() {
        List<String> eventDates = (List<String>) ExecutionContext.get("eventDates");

        // Create readers for each event date
        return new ListItemReader<>(eventDates.stream()
                .flatMap(eventDate -> tableConfig.values().stream()
                        .map(config -> createReader(config, eventDate)))
                .toList());
    }

    private JdbcCursorItemReader<Map<String, Object>> createReader(TableConfig config, String eventDate) {
        JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);

        // Load SQL from the external file
        String sql = loadSqlFromFile(config.getSqlFilePath());
        reader.setSql(sql);

        // Set the event date parameter if necessary
        if (config.isUseEventDate()) {
            reader.setPreparedStatementSetter(ps -> {
                ps.setString(1, eventDate); // Set the event date
            });
        }

        reader.setRowMapper(new ColumnMapRowMapper());
        return reader;
    }

    private String loadSqlFromFile(String filePath) {
        try {
            return Files.readString(Paths.get(filePath));
        } catch (Exception e) {
            throw new RuntimeException("Error reading SQL file: " + filePath, e);
        }
    }

    @Bean
    public ItemWriter<Map<String, Object>> tableItemWriter() {
        return items -> {
            for (Map<String, Object> item : items) {
                // Assuming the item contains event date and other necessary information
                String tableName = item.get("tableName").toString(); // Adjust as per your item structure
                String eventDate = item.get("eventDate").toString(); // Adjust as per your item structure
                String dateCreated = new SimpleDateFormat("yyyyMMdd").format(new Date()); // Current date for filename
                
                String outputFilePath = String.format("output/%s_%s_%s.dat", tableName, eventDate, dateCreated);
                FlatFileItemWriter<Map<String, Object>> writer = createFileWriter(outputFilePath);
                writer.open(new ExecutionContext());
                writer.write(List.of(item));
                writer.close();
            }
        };
    }

    private FlatFileItemWriter<Map<String, Object>> createFileWriter(String outputFilePath) {
        return new FlatFileItemWriterBuilder<Map<String, Object>>()
                .name("tableWriter")
                .resource(new FileSystemResource(outputFilePath))
                .lineAggregator(createLineAggregator())
                .build();
    }

    private LineAggregator<Map<String, Object>> createLineAggregator() {
        return item -> String.join("|", item.values().stream()
                .map(String::valueOf)
                .toArray(String[]::new));
    }
}
