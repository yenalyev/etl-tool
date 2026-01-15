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
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.PlatformTransactionManager;

import java.sql.SQLException;
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
     *
     * 🔴 ПРОБЛЕМА В СТАРОМУ КОНФІГІ:
     * - skipLimit(100) + skip(Exception.class) = пропускає ВСІ помилки до 100 разів
     * - Це означає що навіть якщо PostgreSQL недоступний, Spring Batch буде
     *   намагатися 100 разів створити connection pool
     * - Команда STOP не працює бо job зациклений на retry
     *
     * ✅ ВИПРАВЛЕННЯ:
     * - Зменшено skipLimit до 10
     * - Skip тільки для data validation errors (некоректні дані в рядках)
     * - noSkip для ФАТАЛЬНИХ помилок (connection errors, IllegalStateException)
     * - Додано retry policy: максимум 2 спроби, тільки для transient errors
     * - noRetry для connection errors - job падає МИТТЄВО
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

                // ═══════════════════════════════════════════════════════════
                // FAULT TOLERANCE CONFIGURATION
                // ═══════════════════════════════════════════════════════════
                .faultTolerant()

                // ───────────────────────────────────────────────────────────
                // SKIP POLICY: Пропускаємо ТІЛЬКИ data validation errors
                // ───────────────────────────────────────────────────────────

                // Максимум 10 пропущених рядків (замість 100)
                .skipLimit(10)

                // ✅ SKIP: Пропускаємо тільки data errors (проблеми з даними в рядках)
                .skip(DataIntegrityViolationException.class)  // Дублікати, constraint violations

                // ❌ NO SKIP: НЕ пропускаємо фатальні помилки
                .noSkip(RuntimeException.class)                      // Загальні runtime помилки
                .noSkip(IllegalStateException.class)                 // Shutdown, invalid state
                .noSkip(SQLException.class)                          // Database connection problems
                .noSkip(DataAccessResourceFailureException.class)    // Connection pool exhausted
                .noSkip(IllegalArgumentException.class)              // Invalid configuration

                // ───────────────────────────────────────────────────────────
                // RETRY POLICY: Повторюємо ТІЛЬКИ transient errors
                // ───────────────────────────────────────────────────────────

                // Максимум 2 retry (загалом 3 спроби: original + 2 retry)
                .retryLimit(2)

                // ✅ RETRY: Повторюємо тільки тимчасові проблеми
                .retry(OptimisticLockingFailureException.class)      // Concurrent update conflicts
                .retry(DeadlockLoserDataAccessException.class)       // Database deadlocks

                // ❌ NO RETRY: НЕ повторюємо фатальні помилки
                .noRetry(SQLException.class)                         // Connection errors
                .noRetry(DataAccessResourceFailureException.class)   // Pool exhausted
                .noRetry(IllegalStateException.class)                // Shutdown/invalid state
                .noRetry(RuntimeException.class)                     // Інші runtime помилки

                .build();
    }

    /**
     * ✅ Virtual Threads TaskExecutor для Spring Batch
     */
    @Bean(name = "batchTaskExecutor")
    public TaskExecutor batchTaskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("batch-vt-");
        executor.setVirtualThreads(true);
        executor.setConcurrencyLimit(-1);
        return executor;
    }
}