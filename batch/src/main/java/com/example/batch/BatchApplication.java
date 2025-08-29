package com.example.batch;

import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.JobExecutionEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;

import javax.sql.DataSource;
import java.util.Map;

import static org.springframework.web.servlet.function.RouterFunctions.route;

@ImportRuntimeHints(BatchApplication.Hints.class)
@SpringBootApplication
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

    static final Resource SAMPLE_CSV = new ClassPathResource("sample.csv");

    static final String PREPARE_DB_STEP = "prepareDbStep";
    static final String ETL_STEP = "etlStep";

    @Bean
    FlatFileItemReader<Customer> itemReader() {
        return new FlatFileItemReaderBuilder<Customer>()
                .resource(SAMPLE_CSV)
                .name("itemReader")
                .linesToSkip(1)
                .delimited().delimiter(",")
                .names("id", "name")
                .fieldSetMapper(fieldSet -> new Customer(fieldSet.readInt("id"), fieldSet.readString("name")))
                .build();
    }

    @Bean
    ItemProcessor<Customer, UppercaseCustomer> itemProcessor() {
        return item -> new UppercaseCustomer(item.id(), item.name().toUpperCase());
    }

    @Bean
    ItemWriter<UppercaseCustomer> itemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<UppercaseCustomer>()
                .dataSource(dataSource)
                .sql("INSERT INTO customer (id, name) VALUES (?,?)")
                .itemPreparedStatementSetter((item, ps) -> {
                    ps.setInt(1, item.id());
                    ps.setString(2, item.name());
                })
                .build();
    }

    @Bean(ETL_STEP)
    Step etlStep(JobRepository repository,
                 ItemReader<Customer> itemReader,
                 ItemWriter<UppercaseCustomer> itemWriter,
                 ItemProcessor<Customer, UppercaseCustomer> itemProcessor,
                 PlatformTransactionManager transactionManager) {
        return new StepBuilder("step1", repository)
                .<Customer, UppercaseCustomer>chunk(10, transactionManager)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();
    }

    @Bean(PREPARE_DB_STEP)
    Step prepareDbStep(JobRepository repository,
                       JdbcClient db,
                       PlatformTransactionManager tx,
                       JobRepository jobRepository) {
        return new StepBuilder("prepareDbStep", jobRepository)
                .tasklet((_, _) -> {
                    db.sql("delete from customer").update();
                    return RepeatStatus.FINISHED;
                }, tx)
                .build();
    }



    @Bean
    Job job(JobRepository repository, @Qualifier(PREPARE_DB_STEP) Step p, @Qualifier(ETL_STEP) Step e) {
        return new JobBuilder("job", repository)
                .start(p)
                .next(e)
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @EventListener
    void afterJob(JobExecutionEvent jobExecution) {
        System.out.println("finished the batch job! " + jobExecution.getJobExecution().getExitStatus());
    }


    @Bean
    RouterFunction<ServerResponse> httpServer(JobLauncher jobLauncher, Job job) {
        return route()
                .GET("/job", _ -> {
                    var run = jobLauncher.run(job, new JobParametersBuilder().toJobParameters());
                    while (run.isRunning()) {
                        Thread.sleep(1000);
                    }
                    return ServerResponse.ok().body(Map.of("status", run.getStatus().name()));
                })
                .build();
    }

    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
            hints.resources().registerResource(SAMPLE_CSV);
        }
    }
}

record Customer(int id, String name) {
}

record UppercaseCustomer(int id, String name) {
}