package com.etl.etltool.core.service;

import com.etl.etltool.config.DataSourceManager;
import com.etl.etltool.core.entity.AppConfig;
import com.etl.etltool.core.entity.SyncTask;
import com.etl.etltool.core.entity.TaskExecutionHistory;
import com.etl.etltool.core.service.google.GoogleSheetsService;
import com.etl.etltool.dto.RowChange;
import com.etl.etltool.dto.ValidationResult;
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
 * ✅ Сервіс для запуску Spring Batch jobs з валідацією даних та approval
 *
 * НОВИЙ WORKFLOW:
 * 1. Завантаження даних з Google Sheets
 * 2. Збереження CSV snapshot
 * 3. Валідація та порівняння з попередніми даними
 * 4. Якщо зміни виявлені → очікування approval від користувача
 * 5. Після approval → запуск Spring Batch job
 * 6. Збереження результату в історію
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
    private final DataSourceManager dataSourceManager;
    private final GoogleSheetsService googleSheetsService;
    private final CsvStorageService csvStorageService;
    private final DataValidationService dataValidationService;
    private final TaskExecutionHistoryService historyService;

    /**
     * Запуск задачі з валідацією даних
     *
     * workflow включає:
     * 1. Pre-flight validation (БД, конфігурація)
     * 2. Завантаження даних з Google Sheets
     * 3. Збереження CSV snapshot
     * 4. Валідація та порівняння
     * 5. Якщо потрібен approval → очікування
     * 6. Запуск batch job
     */
    @Async("etlTaskExecutor")
    public void runTaskAsync(Long taskId) {
        String threadName = Thread.currentThread().getName();
        log.info("═══════════════════════════════════════════════════════");
        log.info("🚀 Starting async task {} on thread: {}", taskId, threadName);
        log.info("═══════════════════════════════════════════════════════");

        TaskExecutionHistory historyRecord = null;

        try {
            // ═══════════════════════════════════════════════════════════
            // КРОК 1: Підготовка
            // ═══════════════════════════════════════════════════════════

            executionManager.closeEmitter(taskId);
            executionManager.log(taskId, "🔄 Підготовка до запуску (Потік: " + threadName + ")");

            AppConfig globalConfig = configService.getConfig();
            SyncTask task = taskService.getTask(taskId);

            // ═══════════════════════════════════════════════════════════
            // КРОК 2: Pre-Flight Validation (БД, конфігурація)
            // ═══════════════════════════════════════════════════════════

            executionManager.log(taskId, "🔍 Перевірка підключень...");

            try {
                validatePrerequisites(taskId, task, globalConfig);
                executionManager.log(taskId, "✅ Базова валідація пройдена");
            } catch (PreFlightValidationException e) {
                log.error("❌ Pre-flight validation failed for task: {}", taskId);
                executionManager.log(taskId, "❌ " + e.getMessage());
                executionManager.finish(taskId, false, "Validation failed: " + e.getMessage());
                return;
            }

            // ═══════════════════════════════════════════════════════════
            // КРОК 3: Завантаження даних з Google Sheets
            // ═══════════════════════════════════════════════════════════

            executionManager.log(taskId, "📥 Завантаження даних з Google Sheets...");

            List<List<Object>> rawData = googleSheetsService.readSheet(
                    task.getGoogleSheetId(),
                    task.getSheetName(),
                    globalConfig.getServiceAccountKeyPath()
            );

            if (rawData == null || rawData.isEmpty()) {
                executionManager.log(taskId, "❌ Google Sheets порожній або недоступний");
                executionManager.finish(taskId, false, "No data in Google Sheets");
                return;
            }

            executionManager.log(taskId, String.format("✅ Завантажено %d рядків", rawData.size()));

            // Перший рядок = заголовки
            List<String> headers = rawData.get(0).stream()
                    .map(obj -> obj != null ? obj.toString() : "")
                    .toList();

            // Решта рядків = дані
            List<List<Object>> dataRows = rawData.subList(1, rawData.size());

            // ═══════════════════════════════════════════════════════════
            // КРОК 4: Збереження CSV snapshot
            // ═══════════════════════════════════════════════════════════

            executionManager.log(taskId, "💾 Збереження snapshot...");

            String csvPath = csvStorageService.saveCsv(taskId, headers, dataRows);
            long csvSize = csvStorageService.getStats(taskId).getTotalSizeBytes();

            executionManager.log(taskId, "✅ Snapshot збережено: " + csvPath);

            // ═══════════════════════════════════════════════════════════
            // КРОК 5: Валідація та порівняння даних
            // ═══════════════════════════════════════════════════════════

            executionManager.log(taskId, "🔍 Аналіз змін...");

            ValidationResult validation = dataValidationService.validateData(
                    task, headers, dataRows
            );

            if (!validation.isValid()) {
                // Критичні помилки валідації - блокуємо імпорт
                executionManager.log(taskId, "❌ Критичні помилки валідації:");
                for (String error : validation.getCriticalErrors()) {
                    executionManager.log(taskId, "   • " + error);
                }
                executionManager.finish(taskId, false, "Validation failed");
                return;
            }

            // ═══════════════════════════════════════════════════════════
            // КРОК 6: Відправка звіту про зміни та очікування approval
            // ═══════════════════════════════════════════════════════════

            if (validation.isRequiresApproval()) {
                log.info("📋 Changes detected, sending validation report to user...");

                // Відправляємо детальний звіт через SSE
                sendValidationReport(taskId, validation);

                // Переводимо задачу в режим очікування approval
                executionManager.setWaitingForApproval(taskId, validation);

                executionManager.log(taskId, "⏳ Очікування підтвердження від користувача...");

                log.info("Task {} is waiting for user approval", taskId);

                // Виходимо з методу - продовження після approval через approveTask()
                return;
            }

            // ═══════════════════════════════════════════════════════════
            // КРОК 7: Немає змін або це перший запуск - одразу запускаємо job
            // ═══════════════════════════════════════════════════════════

            executionManager.log(taskId, "✅ Валідація пройшла, запуск імпорту...");

            // Створюємо запис історії
            historyRecord = historyService.startExecution(
                    task, csvPath, csvSize,
                    validation.hasChanges(),
                    buildChangesJson(validation)
            );

            // Запускаємо batch job
            executeJob(taskId, task, globalConfig, historyRecord.getId());

        } catch (JobExecutionAlreadyRunningException e) {
            log.error("❌ Job already running for task: {}", taskId);
            executionManager.log(taskId, "⚠️ Задача вже виконується");
            executionManager.finish(taskId, false, "Задача вже виконується");

        } catch (Exception e) {
            log.error("❌ Failed to start job for task: {}", taskId, e);
            executionManager.log(taskId, "❌ Критична помилка: " + e.getClass().getSimpleName());
            executionManager.log(taskId, "💬 " + e.getMessage());
            executionManager.finish(taskId, false, "Помилка запуску: " + e.getMessage());

            // Оновлюємо історію якщо вона була створена
            if (historyRecord != null) {
                historyService.finishExecution(
                        historyRecord.getId(), "FAILED",
                        null, null, null, e.getMessage(), null
                );
            }

        } finally {
            log.info("═══════════════════════════════════════════════════════");
            log.info("🏁 runTaskAsync() completed for task: {}", taskId);
            log.info("═══════════════════════════════════════════════════════\n");
        }
    }

    /**
     * Підтвердження імпорту користувачем
     * Викликається з API endpoint після approval
     */
    public void approveTask(Long taskId) {
        log.info("✅ Task {} approved by user, starting import...", taskId);

        try {
            // Отримуємо validation result зі стану
            ValidationResult validation = executionManager.getState(taskId).getValidationResult();

            if (validation == null) {
                log.error("Validation result not found for task: {}", taskId);
                executionManager.log(taskId, "❌ Помилка: validation result не знайдено");
                executionManager.finish(taskId, false, "Validation result not found");
                return;
            }

            // Завантажуємо конфігурацію
            AppConfig globalConfig = configService.getConfig();
            SyncTask task = taskService.getTask(taskId);

            executionManager.log(taskId, "✅ Підтверджено користувачем, запуск імпорту...");

            // Створюємо запис історії
            TaskExecutionHistory historyRecord = historyService.startExecution(
                    task,
                    validation.getCsvPath(),
                    0L, // Size буде оновлено пізніше
                    validation.hasChanges(),
                    buildChangesJson(validation)
            );

            // Запускаємо batch job
            executeJob(taskId, task, globalConfig, historyRecord.getId());

        } catch (Exception e) {
            log.error("Failed to start approved task: {}", taskId, e);
            executionManager.log(taskId, "❌ Помилка запуску: " + e.getMessage());
            executionManager.finish(taskId, false, "Помилка: " + e.getMessage());
        }
    }

    /**
     * Відхилення імпорту користувачем
     */
    public void rejectTask(Long taskId) {
        log.info("❌ Task {} rejected by user", taskId);

        executionManager.log(taskId, "🛑 Імпорт скасовано користувачем");
        executionManager.finish(taskId, false, "Скасовано користувачем");
    }

    /**
     * Виконання Spring Batch Job
     */
    private void executeJob(
            Long taskId,
            SyncTask task,
            AppConfig globalConfig,
            Long historyId) throws Exception {

        executionManager.log(taskId, "📊 Читання з Google Sheet: " + task.getGoogleSheetId());
        executionManager.log(taskId, "🗄️ Цільова таблиця: " + task.getTargetTableName());

        // Створюємо JobParameters з унікальністю
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("taskId", taskId)
                .addLong("historyId", historyId)
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

        log.info("📋 Job parameters created");

        // Запускаємо Spring Batch Job
        JobExecution jobExecution = jobLauncher.run(etlJob, jobParameters);
        executionManager.registerJobExecution(taskId, jobExecution);

        log.info("✅ Job execution started: ID={}, Status={}",
                jobExecution.getJobId(), jobExecution.getStatus());
    }

    /**
     * Відправка детального звіту про зміни через SSE
     */
    private void sendValidationReport(Long taskId, ValidationResult validation) {
        executionManager.log(taskId, "");
        executionManager.log(taskId, "╔══════════════════════════════════════════════════════╗");
        executionManager.log(taskId, "║  📋 ЗВІТ ПРО ЗМІНИ                                  ║");
        executionManager.log(taskId, "╚══════════════════════════════════════════════════════╝");
        executionManager.log(taskId, "");

        // Основна інформація
        if (validation.isFirstRun()) {
            executionManager.log(taskId, "🆕 Це перший імпорт для цієї задачі");
            executionManager.log(taskId, String.format("📊 Буде імпортовано: %d рядків", validation.getTotalRows()));
        } else {
            executionManager.log(taskId, String.format("📊 Всього рядків: %d", validation.getTotalRows()));
            executionManager.log(taskId, String.format("   ├─ 🆕 Нових: %d", validation.getNewRows()));
            executionManager.log(taskId, String.format("   ├─ ✏️ Змінених: %d", validation.getModifiedRows()));
            executionManager.log(taskId, String.format("   ├─ 🗑️ Видалених: %d", validation.getDeletedRows()));
            executionManager.log(taskId, String.format("   └─ ✅ Без змін: %d", validation.getUnchangedRows()));
        }

        executionManager.log(taskId, "");

        // Попередження про структуру
        if (!validation.getMissingColumns().isEmpty()) {
            executionManager.log(taskId, "❌ КРИТИЧНО: Відсутні колонки яки ми мапимо:");
            for (String col : validation.getMissingColumns()) {
                executionManager.log(taskId, "   • " + col);
            }
            executionManager.log(taskId, "");
        }

        if (!validation.getNewColumns().isEmpty()) {
            executionManager.log(taskId, "⚠️ Знайдено колонки в Google Sheets яких немає в мапінгу (не аналізуємо):");
            for (String col : validation.getNewColumns()) {
                if (col.isEmpty()){
                    col = "Колонка без заголовку";
                }
                executionManager.log(taskId, "   • " + col);
            }
            executionManager.log(taskId, "");
        }

        // Приклади змін
        if (!validation.getSampleChanges().isEmpty() && !validation.isFirstRun()) {
            executionManager.log(taskId, "📝 Приклади змін (перші 5):");
            executionManager.log(taskId, "");

            for (RowChange change : validation.getSampleChanges()) {
                String typeEmoji = switch (change.getChangeType()) {
                    case "NEW" -> "🆕";
                    case "MODIFIED" -> "✏️";
                    case "DELETED" -> "🗑️";
                    default -> "•";
                };

                executionManager.log(taskId, String.format("%s Рядок #%d (%s):",
                        typeEmoji, change.getRowIndex() + 1, change.getChangeType()));

                // Sample data
                if (change.getSampleData() != null && !change.getSampleData().isEmpty()) {
                    for (var entry : change.getSampleData().entrySet()) {
                        executionManager.log(taskId, String.format("     %s: %s",
                                entry.getKey(), entry.getValue()));
                    }
                }

                executionManager.log(taskId, "");
            }
        }

        // Інші попередження
        if (!validation.getWarnings().isEmpty()) {
            executionManager.log(taskId, "⚠️ Попередження:");
            for (String warning : validation.getWarnings()) {
                executionManager.log(taskId, "   • " + warning);
            }
            executionManager.log(taskId, "");
        }

        executionManager.log(taskId, "╔══════════════════════════════════════════════════════╗");
        executionManager.log(taskId, "║  ⏳ Очікування вашого рішення...                    ║");
        executionManager.log(taskId, "║  👉 Натисніть 'Continue' для продовження            ║");
        executionManager.log(taskId, "║  👉 Натисніть 'Cancel' для скасування               ║");
        executionManager.log(taskId, "╚══════════════════════════════════════════════════════╝");
        executionManager.log(taskId, "");
    }

    /**
     * Побудова JSON зі змінами для збереження в історію
     */
    private String buildChangesJson(ValidationResult validation) {
        return String.format(
                "{\"newRows\":%d,\"modifiedRows\":%d,\"deletedRows\":%d,\"unchangedRows\":%d}",
                validation.getNewRows(),
                validation.getModifiedRows(),
                validation.getDeletedRows(),
                validation.getUnchangedRows()
        );
    }

    /**
     * PRE-FLIGHT VALIDATION: Перевірка prerequisites перед запуском job
     */
    private void validatePrerequisites(Long taskId, SyncTask task, AppConfig config)
            throws PreFlightValidationException {

        log.info("┌─────────────────────────────────────────────────");
        log.info("│ 🔍 PRE-FLIGHT VALIDATION for task: {}", taskId);
        log.info("└─────────────────────────────────────────────────");

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

        executionManager.log(taskId, "   → Перевірка підключення до БД...");

        try {
            DataSource dataSource = dataSourceManager.getDataSource(
                    config.getTargetDbUrl(),
                    config.getTargetDbUser(),
                    config.getTargetDbPassword()
            );

            try (Connection conn = dataSource.getConnection()) {
                if (conn == null || conn.isClosed()) {
                    throw new PreFlightValidationException("❌ Не вдалося отримати з'єднання з БД");
                }

                String dbProduct = conn.getMetaData().getDatabaseProductName();
                String dbVersion = conn.getMetaData().getDatabaseProductVersion();

                log.info("✅ Database connection OK: {} {}", dbProduct, dbVersion);
                executionManager.log(taskId, "   ✓ БД доступна: " + dbProduct + " " + dbVersion);

            } catch (SQLException e) {
                log.error("❌ Database connection test failed", e);
                throw new PreFlightValidationException("❌ БД недоступна: " + e.getMessage());
            }

        } catch (IllegalStateException e) {
            log.error("❌ Cannot validate - application shutting down", e);
            throw new PreFlightValidationException("❌ Додаток зупиняється");

        } catch (RuntimeException e) {
            log.error("❌ Failed to create connection pool", e);

            String errorMsg = e.getMessage();

            if (errorMsg != null &&
                    (errorMsg.contains("Connection") ||
                            errorMsg.contains("refused") ||
                            errorMsg.contains("Failed to initialize pool") ||
                            errorMsg.contains("postmaster"))) {

                String dbUrl = config.getTargetDbUrl();
                throw new PreFlightValidationException(
                        "❌ Неможливо підключитися до БД.\n" +
                                "   Перевірте що PostgreSQL запущений: " + dbUrl
                );
            }

            throw new PreFlightValidationException("❌ Помилка підключення до БД: " + errorMsg);
        }

        log.info("✅ Pre-flight validation completed successfully");
        log.info("─────────────────────────────────────────────────\n");
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
                if (executionManager.initTask(task.getId())) {
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
     * Зупинка задачі через JobOperator
     */
    public boolean stopTask(Long taskId) {
        log.info("═══════════════════════════════════════════════════════");
        log.info("🛑 Stop request for task: {}", taskId);
        log.info("═══════════════════════════════════════════════════════");

        try {
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
                executionManager.log(taskId, "⚠️ Задача вже не виконується");
                return false;
            }

            log.info("🛑 Stopping JobExecution: {}", executionId);

            executionManager.log(taskId, "🛑 Надіслано команду зупинки...");
            executionManager.log(taskId, "⏳ Очікування завершення поточного chunk...");

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
            log.error("❌ JobExecution not found for task: {}", taskId, e);
            executionManager.log(taskId, "❌ Job execution не знайдено");
            return false;

        } catch (org.springframework.batch.core.launch.JobExecutionNotRunningException e) {
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
     * Custom Exception для Pre-Flight Validation
     */
    private static class PreFlightValidationException extends Exception {
        public PreFlightValidationException(String message) {
            super(message);
        }
    }
}