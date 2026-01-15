package com.etl.etltool.core.service;

import com.etl.etltool.config.DataSourceManager;
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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * ✅ Сервіс для запуску Spring Batch jobs з Pre-Flight Validation
 *
 * Основні можливості:
 * - Перевірка підключення до БД ПЕРЕД запуском job
 * - Закриття SSE перед повторним запуском
 * - Graceful stop через JobOperator
 * - Детальне логування в SSE
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
    private final DataSourceManager dataSourceManager; // ✅ ДОДАНО для pre-flight validation

    /**
     * ✅ Запуск однієї задачі асинхронно з Pre-Flight Validation
     *
     * Послідовність:
     * 1. Закриття старих SSE з'єднань
     * 2. Завантаження конфігурації
     * 3. ✨ PRE-FLIGHT VALIDATION (перевірка БД)
     * 4. Запуск Spring Batch Job
     */
    @Async("etlTaskExecutor")
    public void runTaskAsync(Long taskId) {
        String threadName = Thread.currentThread().getName();
        log.info("═══════════════════════════════════════════════════════");
        log.info("🚀 Starting async task {} on thread: {}", taskId, threadName);
        log.info("═══════════════════════════════════════════════════════");

        try {
            // ═══════════════════════════════════════════════════════════
            // КРОК 1: Підготовка
            // ═══════════════════════════════════════════════════════════

            // Закриваємо старі SSE з'єднання перед запуском
            executionManager.closeEmitter(taskId);

            executionManager.log(taskId, "🔄 Підготовка до запуску (Потік: " + threadName + ")");

            // Завантажуємо конфігурацію
            AppConfig globalConfig = configService.getConfig();
            SyncTask task = taskService.getTask(taskId);

            // ═══════════════════════════════════════════════════════════
            // КРОК 2: ✨ PRE-FLIGHT VALIDATION
            // Перевіряємо всі необхідні умови ПЕРЕД запуском job
            // ═══════════════════════════════════════════════════════════

            executionManager.log(taskId, "🔍 Перевірка підключень...");

            try {
                validatePrerequisites(taskId, task, globalConfig);
                executionManager.log(taskId, "✅ Всі перевірки пройдено успішно");

            } catch (PreFlightValidationException e) {
                // Якщо валідація не пройшла - зупиняємо НЕГАЙНО
                // Job взагалі не запускається
                log.error("❌ Pre-flight validation failed for task: {}", taskId);
                log.error("❌ Reason: {}", e.getMessage());

                executionManager.log(taskId, "❌ " + e.getMessage());
                executionManager.finish(taskId, false, "Validation failed: " + e.getMessage());

                return; // ← Виходимо з методу, job НЕ запускається
            }

            // ═══════════════════════════════════════════════════════════
            // КРОК 3: Запуск Spring Batch Job
            // ═══════════════════════════════════════════════════════════

            executionManager.log(taskId, "📊 Читання з Google Sheet: " + task.getGoogleSheetId());
            executionManager.log(taskId, "🗄️ Цільова таблиця: " + task.getTargetTableName());

            // Створюємо JobParameters з унікальністю
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
     * ✅ PRE-FLIGHT VALIDATION: Перевірка всіх prerequisites перед запуском job
     *
     * Перевіряємо:
     * 1. Конфігурацію (чи всі поля заповнені)
     * 2. Підключення до БД (чи доступна база даних)
     *
     * @throws PreFlightValidationException якщо щось не так
     */
    private void validatePrerequisites(Long taskId, SyncTask task, AppConfig config)
            throws PreFlightValidationException {

        log.info("┌─────────────────────────────────────────────────");
        log.info("│ 🔍 PRE-FLIGHT VALIDATION for task: {}", taskId);
        log.info("└─────────────────────────────────────────────────");

        // ═══════════════════════════════════════════════════════════
        // 1. Перевірка конфігурації
        // ═══════════════════════════════════════════════════════════

        executionManager.log(taskId, "   → Перевірка конфігурації...");

        if (config.getServiceAccountKeyPath() == null || config.getServiceAccountKeyPath().isEmpty()) {
            throw new PreFlightValidationException("❌ Google Service Account key не налаштовано");
        }

        if (config.getTargetDbUrl() == null || config.getTargetDbUrl().isEmpty()) {
            throw new PreFlightValidationException("❌ Database URL не налаштовано");
        }

        if (task.getGoogleSheetId() == null || task.getGoogleSheetId().isEmpty()) {
            throw new PreFlightValidationException("❌ Google Sheet ID не вказано");
        }

        if (task.getTargetTableName() == null || task.getTargetTableName().isEmpty()) {
            throw new PreFlightValidationException("❌ Ім'я цільової таблиці не вказано");
        }

        if (task.getFieldMappingJson() == null || task.getFieldMappingJson().isEmpty()) {
            throw new PreFlightValidationException("❌ Field mapping не налаштовано");
        }

        log.info("✅ Configuration check passed");

        // ═══════════════════════════════════════════════════════════
        // 2. Перевірка підключення до БД
        // ═══════════════════════════════════════════════════════════

        executionManager.log(taskId, "   → Перевірка підключення до БД...");

        try {
            // Отримуємо або створюємо DataSource
            DataSource dataSource = dataSourceManager.getDataSource(
                    config.getTargetDbUrl(),
                    config.getTargetDbUser(),
                    config.getTargetDbPassword()
            );

            // Тестуємо з'єднання
            try (Connection conn = dataSource.getConnection()) {
                if (conn == null || conn.isClosed()) {
                    throw new PreFlightValidationException(
                            "❌ Не вдалося отримати з'єднання з БД"
                    );
                }

                // Отримуємо інформацію про БД
                String dbProduct = conn.getMetaData().getDatabaseProductName();
                String dbVersion = conn.getMetaData().getDatabaseProductVersion();

                log.info("✅ Database connection OK: {} {}", dbProduct, dbVersion);
                executionManager.log(taskId, "   ✓ БД доступна: " + dbProduct + " " + dbVersion);

            } catch (SQLException e) {
                log.error("❌ Database connection test failed", e);
                throw new PreFlightValidationException(
                        "❌ БД недоступна: " + e.getMessage()
                );
            }

        } catch (IllegalStateException e) {
            // Якщо додаток зупиняється під час перевірки
            log.error("❌ Cannot validate - application shutting down", e);
            throw new PreFlightValidationException("❌ Додаток зупиняється");

        } catch (RuntimeException e) {
            // Connection pool creation failed
            log.error("❌ Failed to create connection pool", e);

            String errorMsg = e.getMessage();

            // Перевіряємо чи це connection error
            if (errorMsg != null &&
                    (errorMsg.contains("Connection") ||
                            errorMsg.contains("refused") ||
                            errorMsg.contains("Failed to initialize pool") ||
                            errorMsg.contains("postmaster"))) {

                // Формуємо зрозуміле повідомлення для користувача
                String dbUrl = config.getTargetDbUrl();
                throw new PreFlightValidationException(
                        "❌ Неможливо підключитися до БД.\n" +
                                "   Перевірте що PostgreSQL запущений: " + dbUrl
                );
            }

            // Інша помилка
            throw new PreFlightValidationException(
                    "❌ Помилка підключення до БД: " + errorMsg
            );
        }

        log.info("✅ Pre-flight validation completed successfully");
        log.info("─────────────────────────────────────────────────\n");
    }

    /**
     * ✅ Запуск всіх активних задач
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
     * ✅ Зупинка задачі через JobOperator
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

            // Використовуємо JobOperator для зупинки
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
     * ✅ Custom Exception для Pre-Flight Validation
     *
     * Використовується для сигналізації про проблеми, виявлені
     * під час перевірки prerequisites перед запуском job
     */
    private static class PreFlightValidationException extends Exception {
        public PreFlightValidationException(String message) {
            super(message);
        }
    }
}