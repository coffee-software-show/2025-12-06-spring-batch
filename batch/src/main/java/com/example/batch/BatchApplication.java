package com.example.batch;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.batch.autoconfigure.JobExecutionEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@ImportRuntimeHints(BatchApplication.Hints.class)
@SpringBootApplication
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

    @EventListener
    void onApplicationEvent(JobExecutionEvent event) {
        IO.println("the JobExecutionEvent received: " + event.getJobExecution().getId());
    }

    record Customer(int id, String name) {
    }

    static final Resource RESOURCE = new ClassPathResource("/customers.csv");

    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
            hints.resources().registerResource(RESOURCE);
        }
    }

    @Bean
    FlatFileItemReader<@NonNull Customer> reader() {
        return new FlatFileItemReaderBuilder<@NonNull Customer>()
                .resource(RESOURCE)
                .fieldSetMapper(fieldSet -> new Customer(fieldSet.readInt("id"), fieldSet.readString("name")))
                .linesToSkip(1)
                .name("reader")
                .delimited(d -> d.names("id,name".split(",")))
                .build();
    }

    @Bean
    AsyncTaskExecutor taskExecutor() {
        return new VirtualThreadTaskExecutor();
    }

    @Bean
    Step step2(JobRepository repository, AsyncTaskExecutor taskExecutor, ItemReader<@NonNull Customer> customerItemReader, PlatformTransactionManager transactionManager) {
        return new StepBuilder(repository)
                .<Customer, Customer>chunk(10)
                .transactionManager(transactionManager)
                .reader(customerItemReader)
                .writer(chunk -> chunk.forEach(IO::println))
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    Step step1(JobRepository repository, PlatformTransactionManager transactionManager) {
        return new StepBuilder(repository)
                .tasklet((_, _) -> {
                    IO.println("Hello World");
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    Job job(JobRepository repository, Step step1, Step step2) {
        return new JobBuilder(repository)
                .start(step1)
                .next(step2)
                .incrementer(new RunIdIncrementer())
                .build();

    }
}
