package com.example.batch;

import org.jspecify.annotations.NonNull;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.batch.core.configuration.support.JdbcDefaultBatchConfiguration;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Map;

@EnableJdbcJobRepository
@SpringBootApplication
public class BatchApplication extends JdbcDefaultBatchConfiguration {


    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }


    record Customer(int id, String name) {
    }

    static final Resource IN = new ClassPathResource("customers.csv");

    @Bean
    FlatFileItemReader<@NonNull Customer> reader() {
        return new FlatFileItemReaderBuilder<@NonNull Customer>()
                .resource(IN)
                .linesToSkip(1)
                .delimited(c -> c.names("id,name".split(",")))
                .name("customer")
                .fieldSetMapper(fieldSet -> new Customer(fieldSet.readInt("id"), fieldSet.readString("name")))
                .build();
    }

    @Bean
    Job job(JobRepository repo, PlatformTransactionManager manager, FlatFileItemReader<@NonNull Customer> reader) {
        return new JobBuilder("job", repo)
                .start(new StepBuilder("s1", repo)
                        .<Customer, Customer>chunk(10)
                        .transactionManager(manager)
                        .reader(reader)
                        .writer(chunk -> chunk.forEach(IO::println))
                        .build())
                .incrementer(new RunIdIncrementer())
                .build();

    }

    void enumerate(Map<String, ?> map) {
        map.forEach((key, value) -> {
            IO.println(key + ": " + value);
        });
    }

    @Bean
    ApplicationRunner runner(Map<String, DataSource> dataSources,
                             Map<String, PlatformTransactionManager> transactionManagers
    ) {
        return args -> {
            this.enumerate(dataSources);
            this.enumerate(transactionManagers);
        };
    }
}
