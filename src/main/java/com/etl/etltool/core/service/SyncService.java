package com.etl.etltool.core.service;

import com.etl.etltool.core.entity.AppConfig;
import com.etl.etltool.core.entity.SyncTask;
import com.etl.etltool.core.service.google.GoogleSheetsService;
import com.etl.etltool.model.SheetData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class SyncService {

    private final ConfigService configService;
    private final TaskService taskService;
    private final GoogleSheetsService googleSheetsService;
    private final DatabaseService databaseService;
    private final TaskExecutionManager executionManager; // Додано

    // Запуск ВСІХ задач (проходить циклом і запускає кожну в окремому потоці)
    public void runAllActiveTasks() {
        List<SyncTask> tasks = taskService.getAllActiveTasks();
        if (tasks.isEmpty()) {
            log.info("Немає активних задач.");
            return;
        }
        for (SyncTask task : tasks) {
            // Ініціалізуємо статус перед запуском
            executionManager.initTask(task.getId());
            // Запускаємо асинхронно
            runTaskAsync(task.getId());
        }
    }

    // Основний метод. @Async змушує його виконуватись у фоновому потоці.
    @Async("etlTaskExecutor")
    public void runTaskAsync(Long taskId) {
        String threadName = Thread.currentThread().getName();
        log.info("Async start task {} on {}", taskId, threadName);
        executionManager.log(taskId, "🚀 Старт обробки (Потік: " + threadName + ")");

        try {
            AppConfig globalConfig = configService.getConfig();
            SyncTask task = taskService.getTask(taskId);

            executionManager.log(taskId, "📊 Читання Google Sheet: " + task.getGoogleSheetId());

            // 1. Читання
            List<List<Object>> rawData = googleSheetsService.readSheet(
                    task.getGoogleSheetId(),
                    task.getSheetName(),
                    globalConfig.getServiceAccountKeyPath()
            );

            if (rawData == null || rawData.isEmpty()) {
                executionManager.log(taskId, "⚠️ Дані відсутні або файл пустий.");
                executionManager.finish(taskId, false, "Пустий файл");
                return;
            }

            executionManager.log(taskId, "📥 Отримано рядків: " + rawData.size());

            // 2. Конвертація (Ваш метод без змін)
            SheetData processedData = convertToSheetData(rawData);

            // 3. Запис
            executionManager.log(taskId, "🗄️ Запис у БД: " + task.getTargetTableName());

            databaseService.saveData(
                    processedData,
                    task.getTargetTableName(),
                    task.getFieldMappingJson(),
                    task.isCreateNewTable(),
                    globalConfig.getTargetDbUrl(),
                    globalConfig.getTargetDbUser(),
                    globalConfig.getTargetDbPassword(),
                    globalConfig.getDefaultChunkSize()
            );

            int count = processedData.getRows() != null ? processedData.getRows().size() : 0;
            executionManager.updateProgress(taskId, count);
            executionManager.finish(taskId, true, "Успішно імпортовано " + count + " записів.");

        } catch (Exception e) {
            log.error("Task failed", e);
            executionManager.finish(taskId, false, "Помилка: " + e.getMessage());
        }
    }

    // Ваш допоміжний метод (залишився без змін)
    private SheetData convertToSheetData(List<List<Object>> rawData) {
        if (rawData.size() < 2) {
            return new SheetData(Collections.emptyList(), Collections.emptyList());
        }
        List<String> headers = rawData.get(0).stream()
                .map(obj -> obj != null ? obj.toString() : "")
                .collect(Collectors.toList());
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 1; i < rawData.size(); i++) {
            List<Object> rowRaw = rawData.get(i);
            Map<String, Object> rowMap = new LinkedHashMap<>();
            for (int j = 0; j < headers.size(); j++) {
                String header = headers.get(j);
                Object value = (j < rowRaw.size()) ? rowRaw.get(j) : null;
                rowMap.put(header, value);
            }
            rows.add(rowMap);
        }
        return new SheetData(headers, rows);
    }
}