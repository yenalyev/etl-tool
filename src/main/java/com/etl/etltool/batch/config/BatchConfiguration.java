package com.etl.etltool.batch.config;

import com.etl.etltool.batch.listener.BatchChunkListener;
import com.etl.etltool.batch.listener.BatchJobListener;
import com.etl.etltool.batch.processor.DataValidationProcessor;
import com.etl.etltool.batch.reader.GoogleSheetsItemReader;
import com.etl.etltool.batch.writer.DatabaseItemWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Map;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class BatchConfiguration {

    private final BatchJobListener batchJobListener;
    private final BatchChunkListener batchChunkListener;

    /**
     * ✅ Основний ETL Job
     */
    @Bean(name = "etlJob")
    public Job etlJob(JobRepository jobRepository, Step etlStep) {
        return new JobBuilder("etlJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(etlStep)
                .listener(batchJobListener)
                .build();
    }

    /**
     * ✅ Крок обробки даних (Reader → Processor → Writer)
     */
    @Bean
    public Step etlStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            GoogleSheetsItemReader reader,
            DataValidationProcessor processor,
            DatabaseItemWriter writer) {

        return new StepBuilder("etlStep", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                //.taskExecutor(batchTaskExecutor())
                .listener(batchChunkListener)
                .faultTolerant()
                .skipLimit(100)
                .skip(Exception.class)
                .noSkip(IllegalArgumentException.class)
                .build();
    }

    /**
     * ✅ Virtual Threads TaskExecutor для Spring Batch
     */
    @Bean(name = "batchTaskExecutor")
    public TaskExecutor batchTaskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("batch-vt-");
        executor.setVirtualThreads(true); // ✅ Використовуємо Virtual Threads
        executor.setConcurrencyLimit(-1); // Необмежена кількість virtual threads
        return executor;
    }
}