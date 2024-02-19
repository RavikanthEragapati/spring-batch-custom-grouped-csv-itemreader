package com.example.batchprocessing.config;

import javax.sql.DataSource;

import com.example.batchprocessing.listner.JobCompletionNotificationListener;
import com.example.batchprocessing.model.Person;
import com.example.batchprocessing.model.Report;
import com.example.batchprocessing.processor.PersonItemProcessor;
import com.example.batchprocessing.reader.custom.GroupedItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import java.util.List;

@Configuration
public class StepItemConfiguration {


    // ITEM READER BEAN
    @Bean
    public GroupedItemReader reader() {
        return new GroupedItemReader<Person>(new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("sample-data.csv"))
                .delimited()
                .names("id", "balance")
                .targetType(Person.class)
                .build());
    }

    // ITEM PROCESSOR BEAN
    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    // ITEM WRITER BEAN
    @Bean
    public JdbcBatchItemWriter<Report> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Report>()
                .sql("INSERT INTO report (id, result) VALUES (:id, :result)")
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }

    //######################################################
    //################ Config Job and Step #################
    //######################################################
    @Bean
    public Job importUserJob(JobRepository jobRepository, Step step1, JobCompletionNotificationListener listener) {
        return new JobBuilder("importUserJob", jobRepository)
                .listener(listener)
                .start(step1)
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository,
                      DataSourceTransactionManager transactionManager,
                      GroupedItemReader<Person> reader,
                      PersonItemProcessor processor,
                      JdbcBatchItemWriter<Report> writer) {

        CompletionPolicy abc = null;
        return new StepBuilder("step1", jobRepository)
                .<List<Person>, Report>chunk(1, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }
}
