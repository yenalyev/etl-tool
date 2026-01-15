package com.etl.etltool.core.service;

import com.etl.etltool.core.entity.AppConfig;
import com.etl.etltool.core.entity.SyncTask;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Сервіс для запуску Spring Batch jobs
 * Закриття SSE перед повторним запуском
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BatchSyncService {

    private final JobLauncher jobLauncher;
    private final Job etlJob;
    private final ConfigService configService;
    private final TaskService taskService;
    private final TaskExecutionManager executionManager;
    private final JobExplorer jobExplorer;
    private final JobOperator jobOperator;

    /**
     * Запуск однієї задачі асинхронно
     */
    @Async("etlTaskExecutor")
    public void runTaskAsync(Long taskId) {
        String threadName = Thread.currentThread().getName();
        log.info("═══════════════════════════════════════════════════════");
        log.info("🚀 Starting async task {} on thread: {}", taskId, threadName);
        log.info("═══════════════════════════════════════════════════════");

        try {
            // Закриваємо старі SSE з'єднання перед запуском
            executionManager.closeEmitter(taskId);

            executionManager.log(taskId, "🔄 Підготовка до запуску (Потік: " + threadName + ")");

            // Завантажуємо конфігурацію
            AppConfig globalConfig = configService.getConfig();
            SyncTask task = taskService.getTask(taskId);

            // Валідація
            validateTask(task, globalConfig);

            executionManager.log(taskId, "📊 Читання з Google Sheet: " + task.getGoogleSheetId());
            executionManager.log(taskId, "🗄️ Цільова таблиця: " + task.getTargetTableName());

            // Створюємо JobParameters з ДОДАТКОВОЮ унікальністю
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("taskId", taskId)
                    .addString("sheetId", task.getGoogleSheetId())
                    .addString("sheetName", task.getSheetName() != null ? task.getSheetName() : "")
                    .addString("keyPath", globalConfig.getServiceAccountKeyPath())
                    .addString("tableName", task.getTargetTableName())
                    .addString("mapping", task.getFieldMappingJson())
                    .addString("createTable", String.valueOf(task.isCreateNewTable()))
                    .addString("dbUrl", globalConfig.getTargetDbUrl())
                    .addString("dbUser", globalConfig.getTargetDbUser())
                    .addString("dbPassword", globalConfig.getTargetDbPassword())
                    .addLong("timestamp", System.currentTimeMillis())
                    .addLong("nanoTime", System.nanoTime())
                    .toJobParameters();

            log.info("📋 Job parameters created:");
            log.info("   Task ID: {}", taskId);
            log.info("   Sheet ID: {}", task.getGoogleSheetId());
            log.info("   Table: {}", task.getTargetTableName());
            log.info("   Timestamp: {}", System.currentTimeMillis());

            // Запускаємо Spring Batch Job
            JobExecution jobExecution = jobLauncher.run(etlJob, jobParameters);

            executionManager.registerJobExecution(taskId, jobExecution);

            log.info("✅ Job execution started: ID={}, Status={}",
                    jobExecution.getJobId(),
                    jobExecution.getStatus());

            // Job запущено асинхронно, Spring Batch сам керує life-cycle
            // Результат обробиться в BatchJobListener

        } catch (JobExecutionAlreadyRunningException e) {
            log.error("❌ Job already running for task: {}", taskId);
            executionManager.log(taskId, "⚠️ Задача вже виконується");
            executionManager.finish(taskId, false, "Задача вже виконується");

        } catch (JobRestartException e) {
            log.error("❌ Job restart failed for task: {}", taskId, e);
            executionManager.log(taskId, "❌ Помилка перезапуску: " + e.getMessage());
            executionManager.finish(taskId, false, "Помилка перезапуску: " + e.getMessage());

        } catch (JobInstanceAlreadyCompleteException e) {
            log.error("❌ Job already completed for task: {} - This should not happen with unique timestamps!", taskId);
            executionManager.log(taskId, "⚠️ Job вже завершено (можлива проблема з JobRepository)");
            executionManager.finish(taskId, false, "Задача вже завершена");

        } catch (Exception e) {
            log.error("❌ Failed to start job for task: {}", taskId, e);
            executionManager.log(taskId, "❌ Критична помилка: " + e.getClass().getSimpleName());
            executionManager.log(taskId, "💬 " + e.getMessage());
            executionManager.finish(taskId, false, "Помилка запуску: " + e.getMessage());

        } finally {
            log.info("═══════════════════════════════════════════════════════");
            log.info("🏁 runTaskAsync() completed for task: {}", taskId);
            log.info("═══════════════════════════════════════════════════════\n");
        }
    }

    /**
     * Запуск всіх активних задач
     */
    public void runAllActiveTasks() {
        List<SyncTask> tasks = taskService.getAllActiveTasks();

        if (tasks.isEmpty()) {
            log.info("ℹ️ No active tasks to run");
            return;
        }

        log.info("═══════════════════════════════════════════════════════");
        log.info("🚀 Starting {} active tasks", tasks.size());
        log.info("═══════════════════════════════════════════════════════");

        int started = 0;
        int skipped = 0;

        for (SyncTask task : tasks) {
            try {
                // Ініціалізуємо статус
                if (executionManager.initTask(task.getId())) {
                    // Запускаємо асинхронно
                    runTaskAsync(task.getId());
                    started++;
                } else {
                    log.warn("⚠️ Task {} is already running, skipping", task.getId());
                    skipped++;
                }
            } catch (Exception e) {
                log.error("❌ Failed to start task: {}", task.getId(), e);
                skipped++;
            }
        }

        log.info("📊 Batch start results: {} started, {} skipped", started, skipped);
        log.info("═══════════════════════════════════════════════════════\n");
    }

    /**
     * ✅ ВИПРАВЛЕНО: Зупинка задачі через JobOperator
     *
     * JobOperator - це правильний Spring Batch спосіб зупинки job'ів.
     * Він забезпечує коректну зміну статусу та оповіщення всіх компонентів.
     */
    public boolean stopTask(Long taskId) {
        log.info("═══════════════════════════════════════════════════════");
        log.info("🛑 Stop request for task: {}", taskId);
        log.info("═══════════════════════════════════════════════════════");

        try {
            // Отримуємо JobExecution з менеджера
            JobExecution jobExecution = executionManager.getJobExecution(taskId);

            if (jobExecution == null) {
                log.warn("⚠️ No active JobExecution found for task: {}", taskId);
                executionManager.log(taskId, "⚠️ Задача не активна або вже завершена");
                return false;
            }

            Long executionId = jobExecution.getId();

            if (!jobExecution.isRunning()) {
                log.warn("⚠️ JobExecution {} is not running (status: {})",
                        executionId, jobExecution.getStatus());
                executionManager.log(taskId, "⚠️ Задача вже не виконується (Status: " + jobExecution.getStatus() + ")");
                return false;
            }

            // Логуємо інформацію про зупинку
            log.info("🛑 Stopping JobExecution:");
            log.info("   Job Execution ID: {}", executionId);
            log.info("   Job ID: {}", jobExecution.getJobId());
            log.info("   Current Status: {}", jobExecution.getStatus());
            log.info("   Start Time: {}", jobExecution.getStartTime());

            // Повідомляємо користувача
            executionManager.log(taskId, "🛑 Надіслано команду зупинки...");
            executionManager.log(taskId, "⏳ Очікування завершення поточного chunk...");

            // ✅ ВИПРАВЛЕННЯ: Використовуємо JobOperator замість jobExecution.stop()
            // JobOperator.stop() - це офіційний Spring Batch метод зупинки
            boolean stopped = jobOperator.stop(executionId);

            if (stopped) {
                log.info("✅ Stop command sent successfully for JobExecution: {}", executionId);
                executionManager.log(taskId, "✅ Команду зупинки прийнято");
            } else {
                log.warn("⚠️ Stop command returned false for JobExecution: {}", executionId);
                executionManager.log(taskId, "⚠️ Не вдалося надіслати команду зупинки");
            }

            log.info("═══════════════════════════════════════════════════════\n");

            return stopped;

        } catch (NoSuchJobExecutionException e) {
            // JobExecution не знайдено в JobRepository
            log.error("❌ JobExecution not found for task: {}", taskId, e);
            executionManager.log(taskId, "❌ Job execution не знайдено в системі");
            return false;

        } catch (org.springframework.batch.core.launch.JobExecutionNotRunningException e) {
            // Job вже не виконується
            log.warn("⚠️ Job is not running for task: {}", taskId, e);
            executionManager.log(taskId, "⚠️ Job вже зупинився");
            return false;

        } catch (Exception e) {
            log.error("❌ Failed to stop task: {}", taskId, e);
            executionManager.log(taskId, "❌ Помилка зупинки: " + e.getMessage());
            return false;
        }
    }

    /**
     * Валідація конфігурації перед запуском
     */
    private void validateTask(SyncTask task, AppConfig config) {
        log.info("🔍 Validating task configuration...");

        if (config.getServiceAccountKeyPath() == null || config.getServiceAccountKeyPath().isEmpty()) {
            throw new IllegalArgumentException("Google Service Account key path not configured");
        }

        if (config.getTargetDbUrl() == null || config.getTargetDbUrl().isEmpty()) {
            throw new IllegalArgumentException("Database URL not configured");
        }

        if (task.getGoogleSheetId() == null || task.getGoogleSheetId().isEmpty()) {
            throw new IllegalArgumentException("Google Sheet ID not specified");
        }

        if (task.getTargetTableName() == null || task.getTargetTableName().isEmpty()) {
            throw new IllegalArgumentException("Target table name not specified");
        }

        if (task.getFieldMappingJson() == null || task.getFieldMappingJson().isEmpty()) {
            throw new IllegalArgumentException("Field mapping not configured");
        }

        log.info("✅ Task configuration validated successfully");
    }
}