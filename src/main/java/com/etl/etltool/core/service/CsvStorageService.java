package com.etl.etltool.core.service;

import com.etl.etltool.dto.csv.CsvData;
import com.etl.etltool.dto.csv.CsvStorageStats;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Сервіс для роботи з CSV snapshot'ами
 *
 * Функціонал:
 * - Збереження даних з Google Sheets в CSV
 * - Завантаження попередніх snapshot'ів
 * - Ротація файлів (зберігання максимум N останніх)
 * - Підрахунок статистики файлів
 */
@Slf4j
@Service
public class CsvStorageService {

    private static final String HISTORY_BASE_DIR = ".etl/history";
    private static final DateTimeFormatter FILE_NAME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");
    private static final int MAX_FILES_PER_TASK = 10; // Зберігаємо максимум 10 останніх

    /**
     * Зберегти дані в CSV файл
     *
     * @param taskId ID задачі
     * @param headers Заголовки колонок
     * @param rows Дані (List<List<Object>>)
     * @return Відносний шлях до збереженого файлу
     */
    public String saveCsv(Long taskId, List<String> headers, List<List<Object>> rows) {
        try {
            // Створюємо директорію для задачі
            Path taskDir = getTaskDirectory(taskId);
            Files.createDirectories(taskDir);

            // Генеруємо ім'я файлу з timestamp
            String fileName = LocalDateTime.now().format(FILE_NAME_FORMATTER) + ".csv";
            Path csvFile = taskDir.resolve(fileName);

            log.info("Saving CSV snapshot: {}", csvFile);

            // Записуємо дані в CSV
            try (CSVWriter writer = new CSVWriter(new FileWriter(csvFile.toFile()))) {
                // Записуємо заголовки
                writer.writeNext(headers.toArray(new String[0]));

                // Записуємо рядки даних
                for (List<Object> row : rows) {
                    String[] rowArray = row.stream()
                            .map(obj -> obj != null ? obj.toString() : "")
                            .toArray(String[]::new);
                    writer.writeNext(rowArray);
                }
            }

            long fileSize = Files.size(csvFile);
            log.info("CSV snapshot saved successfully: {} ({} bytes, {} rows)",
                    fileName, fileSize, rows.size());

            // Виконуємо ротацію файлів
            rotateFiles(taskId);

            // Повертаємо відносний шлях
            return String.format("task_%d/%s", taskId, fileName);

        } catch (IOException e) {
            log.error("Failed to save CSV snapshot for task: {}", taskId, e);
            throw new RuntimeException("Failed to save CSV snapshot: " + e.getMessage(), e);
        }
    }

    /**
     * Завантажити останній збережений CSV для задачі
     *
     * @param taskId ID задачі
     * @return Дані з CSV або Optional.empty() якщо файлів немає
     */
    public Optional<CsvData> loadLatestCsv(Long taskId) {
        try {
            Path taskDir = getTaskDirectory(taskId);

            if (!Files.exists(taskDir)) {
                log.debug("No history directory for task: {}", taskId);
                return Optional.empty();
            }

            // Знаходимо найновіший файл
            Optional<Path> latestFile = Files.list(taskDir)
                    .filter(p -> p.toString().endsWith(".csv"))
                    .max(Comparator.comparing(Path::getFileName));

            if (latestFile.isEmpty()) {
                log.debug("No CSV snapshots found for task: {}", taskId);
                return Optional.empty();
            }

            Path csvFile = latestFile.get();
            log.info("Loading CSV snapshot: {}", csvFile);

            return Optional.of(readCsv(csvFile));

        } catch (IOException e) {
            log.error("Failed to load CSV snapshot for task: {}", taskId, e);
            return Optional.empty();
        }
    }

    /**
     * Прочитати CSV файл
     */
    private CsvData readCsv(Path csvFile) throws IOException {
        try (CSVReader reader = new CSVReader(new FileReader(csvFile.toFile()))) {
            List<String[]> allRows = reader.readAll();

            if (allRows.isEmpty()) {
                throw new IOException("CSV file is empty");
            }

            // Перший рядок = заголовки
            List<String> headers = Arrays.asList(allRows.get(0));

            // Решта рядків = дані
            List<List<Object>> rows = allRows.stream()
                    .skip(1)
                    .map(row -> Arrays.stream(row)
                            .map(s -> (Object) s)
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());

            long fileSize = Files.size(csvFile);

            log.info("CSV loaded: {} headers, {} rows, {} bytes",
                    headers.size(), rows.size(), fileSize);

            return CsvData.builder()
                    .headers(headers)
                    .rows(rows)
                    .filePath(csvFile.toString())
                    .fileSize(fileSize)
                    .build();

        } catch (CsvException e) {
            throw new IOException("Failed to parse CSV: " + e.getMessage(), e);
        }
    }

    /**
     * Ротація файлів: видаляємо найстаріші, залишаємо MAX_FILES_PER_TASK
     */
    private void rotateFiles(Long taskId) throws IOException {
        Path taskDir = getTaskDirectory(taskId);

        List<Path> csvFiles = Files.list(taskDir)
                .filter(p -> p.toString().endsWith(".csv"))
                .sorted(Comparator.comparing(Path::getFileName).reversed())
                .collect(Collectors.toList());

        // Якщо файлів більше ніж MAX_FILES_PER_TASK
        if (csvFiles.size() > MAX_FILES_PER_TASK) {
            log.info("Rotating CSV files for task {}: found {}, keeping {}",
                    taskId, csvFiles.size(), MAX_FILES_PER_TASK);

            // Видаляємо найстаріші
            csvFiles.stream()
                    .skip(MAX_FILES_PER_TASK)
                    .forEach(file -> {
                        try {
                            Files.delete(file);
                            log.debug("Deleted old CSV snapshot: {}", file.getFileName());
                        } catch (IOException e) {
                            log.warn("Failed to delete old CSV file: {}", file, e);
                        }
                    });
        }
    }

    /**
     * Отримати директорію для зберігання CSV задачі
     */
    private Path getTaskDirectory(Long taskId) {
        String userHome = System.getProperty("user.home");
        return Paths.get(userHome, HISTORY_BASE_DIR, "task_" + taskId);
    }

    /**
     * Отримати статистику по збереженим файлам для задачі
     */
    public CsvStorageStats getStats(Long taskId) {
        try {
            Path taskDir = getTaskDirectory(taskId);

            if (!Files.exists(taskDir)) {
                return CsvStorageStats.builder()
                        .taskId(taskId)
                        .totalFiles(0)
                        .totalSizeBytes(0L)
                        .build();
            }

            List<Path> csvFiles = Files.list(taskDir)
                    .filter(p -> p.toString().endsWith(".csv"))
                    .collect(Collectors.toList());

            long totalSize = csvFiles.stream()
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            return 0L;
                        }
                    })
                    .sum();

            Optional<Path> latestFile = csvFiles.stream()
                    .max(Comparator.comparing(Path::getFileName));

            return CsvStorageStats.builder()
                    .taskId(taskId)
                    .totalFiles(csvFiles.size())
                    .totalSizeBytes(totalSize)
                    .latestFileName(latestFile.map(p -> p.getFileName().toString()).orElse(null))
                    .directoryPath(taskDir.toString())
                    .build();

        } catch (IOException e) {
            log.error("Failed to get stats for task: {}", taskId, e);
            return CsvStorageStats.builder()
                    .taskId(taskId)
                    .totalFiles(0)
                    .totalSizeBytes(0L)
                    .build();
        }
    }

    /**
     * Видалити всі CSV файли для задачі
     */
    public void deleteAllCsvForTask(Long taskId) {
        try {
            Path taskDir = getTaskDirectory(taskId);

            if (Files.exists(taskDir)) {
                Files.list(taskDir)
                        .filter(p -> p.toString().endsWith(".csv"))
                        .forEach(file -> {
                            try {
                                Files.delete(file);
                                log.info("Deleted CSV file: {}", file);
                            } catch (IOException e) {
                                log.warn("Failed to delete CSV file: {}", file, e);
                            }
                        });

                // Видаляємо директорію якщо вона порожня
                if (Files.list(taskDir).count() == 0) {
                    Files.delete(taskDir);
                    log.info("Deleted empty task directory: {}", taskDir);
                }
            }
        } catch (IOException e) {
            log.error("Failed to delete CSV files for task: {}", taskId, e);
        }
    }
}