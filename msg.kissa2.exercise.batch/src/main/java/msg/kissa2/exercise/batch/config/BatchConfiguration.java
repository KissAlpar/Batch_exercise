package msg.kissa2.exercise.batch.config;
import com.opencsv.*;
import msg.kissa2.exercise.batch.data.CsvRecord;
import msg.kissa2.exercise.batch.data.Table;
import msg.kissa2.exercise.batch.processor.CsvRecordProcessor;
import msg.kissa2.exercise.batch.reader.CsvItemReader;
import msg.kissa2.exercise.batch.writer.CsvTableWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    private Path path = Paths.get(
            ClassLoader.getSystemResource("simplohist307.csv").toURI()
    );

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    public BatchConfiguration() throws URISyntaxException {
    }

    // define beans...

    // this reader reads String[] as a row from csv
    @Bean
    public ItemReader<CsvRecord> reader() {
        try(Reader reader = Files.newBufferedReader(path)) {
            CSVParser parser = new CSVParserBuilder()
                    .withSeparator(';')
                    .build();
            return new CsvItemReader(reader, parser);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    // this writer can write a table into csv
    @Bean
    public ItemWriter writer() {
        return new CsvTableWriter();
    }

    @Bean
    public ItemProcessor recordProcessor() {
        return new CsvRecordProcessor();
    }

    // define jobs and steps
    @Bean
    public Job segregateHistFile(Step step) {
        return  jobBuilderFactory.get("segregateHistFileJob")
                .incrementer(new RunIdIncrementer())
                //.start(deleteFiles())
                .start(step)
                .build();
    }
    @Bean
    public Step deleteFiles() {
        return new Step() {
            @Override
            public String getName() {
                return "Delete files";
            }

            @Override
            public boolean isAllowStartIfComplete() {
                return false;
            }

            @Override
            public int getStartLimit() {
                return 1;
            }

            @Override
            public void execute(StepExecution stepExecution) throws JobInterruptedException {
                List<File> files = Arrays.asList(
                        new File("resources/TIMPL100.csv"),
                        new File("resources/TIMPL101.csv"),
                        new File("resources/TIMPL102.csv")
                );
                for (File f : files) {
                    if (f.exists()) {
                        f.delete();
                    }
                }
            }
        };
    }

    @Bean
    public Step step(ItemReader<CsvRecord> reader) {
        return  stepBuilderFactory.get("step")
                .chunk(100)
                .reader(reader)
                .processor(recordProcessor())
                .writer(writer())
                .build();
    }
}
