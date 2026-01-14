package com.etl.etltool.core.service;

import com.etl.etltool.core.entity.AppConfig;
import com.etl.etltool.core.entity.SyncTask;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * ✅ Сервіс для запуску Spring Batch jobs
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

    /**
     * Запуск однієї задачі асинхронно
     */
    @Async("etlTaskExecutor")
    public void runTaskAsync(Long taskId) {
        String threadName = Thread.currentThread().getName();
        log.info("🚀 Starting async task {} on thread: {}", taskId, threadName);

        try {
            executionManager.log(taskId, "🔄 Підготовка до запуску (Потік: " + threadName + ")");

            // Завантажуємо конфігурацію
            AppConfig globalConfig = configService.getConfig();
            SyncTask task = taskService.getTask(taskId);

            // Валідація
            validateTask(task, globalConfig);

            executionManager.log(taskId, "📊 Читання з Google Sheet: " + task.getGoogleSheetId());
            executionManager.log(taskId, "🗄️ Цільова таблиця: " + task.getTargetTableName());

            // ✅ Створюємо JobParameters
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
                    .addLong("timestamp", System.currentTimeMillis()) // Для унікальності
                    .toJobParameters();

            // ✅ Запускаємо Spring Batch Job
            JobExecution jobExecution = jobLauncher.run(etlJob, jobParameters);

            log.info("✅ Job execution started: {}", jobExecution.getJobId());

            // Job запущено асинхронно, Spring Batch сам керує life-cycle
            // Результат обробиться в BatchJobListener

        } catch (JobExecutionAlreadyRunningException e) {
            log.error("Job already running for task: {}", taskId);
            executionManager.finish(taskId, false, "Задача вже виконується");
        } catch (JobRestartException e) {
            log.error("Job restart failed for task: {}", taskId, e);
            executionManager.finish(taskId, false, "Помилка перезапуску: " + e.getMessage());
        } catch (JobInstanceAlreadyCompleteException e) {
            log.error("Job already completed for task: {}", taskId);
            executionManager.finish(taskId, false, "Задача вже завершена");
        } catch (Exception e) {
            log.error("Failed to start job for task: {}", taskId, e);
            executionManager.log(taskId, "❌ Критична помилка: " + e.getMessage());
            executionManager.finish(taskId, false, "Помилка запуску: " + e.getMessage());
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

        log.info("🚀 Starting {} active tasks", tasks.size());

        for (SyncTask task : tasks) {
            try {
                // Ініціалізуємо статус
                if (executionManager.initTask(task.getId())) {
                    // Запускаємо асинхронно
                    runTaskAsync(task.getId());
                } else {
                    log.warn("Task {} is already running, skipping", task.getId());
                }
            } catch (Exception e) {
                log.error("Failed to start task: {}", task.getId(), e);
            }
        }
    }

    /**
     * Валідація конфігурації перед запуском
     */
    private void validateTask(SyncTask task, AppConfig config) {
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
    }
}
