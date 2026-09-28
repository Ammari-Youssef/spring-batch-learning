package com.youssef.batch.config;

import com.youssef.batch.person.Person;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class BatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final EntityManagerFactory entityManagerFactory;

    @Bean
    public FlatFileItemReader<Person> reader() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<>();
        reader.setName("personItemReader");
        reader.setResource(new FileSystemResource("src/main/resources/data/people.csv"));
        reader.setLinesToSkip(1); // Skip the header line that contains column names
        reader.setLineMapper(lineMapper());

        return reader;

    }

    @Bean
    @StepScope
    public PersonProcessor processor(@Value("#{jobParameters['failAtRow']?: 0}") long failAtRow) {
        return new PersonProcessor(failAtRow);
    }

    @Bean
    public JpaItemWriter<Person> writer(EntityManagerFactory emf) {
        return new JpaItemWriterBuilder<Person>()
                .entityManagerFactory(emf)
                .build();
    }

    @Bean
    public Step importStep(FlatFileItemReader<Person> reader, PersonProcessor processor, JpaItemWriter<Person> writer) {
        return new StepBuilder("importPeople", jobRepository)
                .<Person, Person>chunk(100, transactionManager) // process 100 records (or rows) at a time
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();

    }

    @Bean
    public Job peopleJob(JobRepository jobRepository, Step step) {
        return new JobBuilder("peopleJob", jobRepository)
                .start(step)
                .build();
    }

    private LineMapper<Person> lineMapper() {
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setStrict(false);
        tokenizer.setNames("name", "age", "city");

        BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Person.class);


        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(tokenizer);

        return lineMapper;
    }

}
