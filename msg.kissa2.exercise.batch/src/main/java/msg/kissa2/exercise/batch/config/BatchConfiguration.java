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
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import msg.kissa2.exercise.batch.writer.DocumentWriter;

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
    public ItemReader reader() {
        try {
            Reader reader = Files.newBufferedReader(path);
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
    public ItemWriter writer() throws Exception {
        //return new CsvTableWriter();
    	return new DocumentWriter();
    }

    /*@Bean
    public ItemProcessor recordProcessor() {
        return new CsvRecordProcessor();
    }*/

    // define jobs and steps
    @Bean
    public Job segregateHistFile() throws Exception {
        return  jobBuilderFactory.get("segregateHistFileJob")
                .incrementer(new RunIdIncrementer())
                .start(
                	new FlowBuilder<Flow>("flow")
                		.start(deleteFiles())
                		.next(step())
                		.build()
                )
                .end()
                .build();
    }
    @Bean
    public Step deleteFiles() {
    	return stepBuilderFactory.get("deleteFIles")
    			.tasklet(new Tasklet() {

					@Override
					public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
							throws Exception {
						List<File> files = Arrays.asList(
		                        new File("TIMPL100.csv"),
		                        new File("TIMPL101.csv"),
		                        new File("TIMPL102.csv")
		                );
		                for (File f : files) {
		                    if (f.exists()) {
		                        f.delete();
		                    }
		                }
						return RepeatStatus.FINISHED;
					}
    			
    			})
    			.build();   
    }

    @Bean
    public Step step() throws Exception {
        return  stepBuilderFactory.get("step")
                .chunk(100)
                .reader(reader())
                //.processor(recordProcessor())
                .writer(writer())
                .build();
    }
}
