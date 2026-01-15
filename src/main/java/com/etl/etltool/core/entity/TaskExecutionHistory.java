package com.etl.etltool.core.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Історія виконання задач імпорту
 * Зберігає інформацію про кожен запуск для аудиту та аналізу
 */
@Entity
@Table(name = "task_execution_history")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskExecutionHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Посилання на задачу
     */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "task_id", nullable = false)
    private SyncTask task;

    /**
     * Час старту виконання
     */
    @Column(nullable = false)
    private LocalDateTime startTime;

    /**
     * Час завершення виконання
     */
    private LocalDateTime endTime;

    /**
     * Статус виконання: SUCCESS, FAILED, STOPPED, CANCELLED
     */
    @Column(nullable = false, length = 20)
    private String status;

    /**
     * Кількість прочитаних рядків з Google Sheets
     */
    private Long rowsRead;

    /**
     * Кількість записаних рядків в БД
     */
    private Long rowsWritten;

    /**
     * Кількість пропущених рядків (помилки валідації)
     */
    private Long rowsSkipped;

    /**
     * Повідомлення про помилку (якщо статус FAILED)
     */
    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    /**
     * Відносний шлях до збереженого CSV файлу
     * Наприклад: "task_123/2025-01-15_143022.csv"
     */
    @Column(length = 500)
    private String csvFilePath;

    /**
     * Розмір файлу в байтах
     */
    private Long csvFileSize;

    /**
     * Чи були виявлені зміни порівняно з попереднім запуском
     */
    private Boolean changesDetected;

    /**
     * JSON з деталями змін (для майбутнього аналізу)
     */
    @Column(columnDefinition = "TEXT")
    private String changesDetails;

    /**
     * ID Spring Batch JobExecution
     */
    private Long batchJobExecutionId;

    /**
     * Тривалість виконання в мілісекундах
     */
    public Long getDurationMs() {
        if (startTime != null && endTime != null) {
            return java.time.Duration.between(startTime, endTime).toMillis();
        }
        return null;
    }

    /**
     * Чи виконання було успішним
     */
    public boolean isSuccessful() {
        return "SUCCESS".equals(status);
    }
}