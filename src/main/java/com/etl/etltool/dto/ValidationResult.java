package com.etl.etltool.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Результат валідації даних перед імпортом
 * Містить інформацію про зміни, які потребують підтвердження користувача
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ValidationResult {

    /**
     * Чи пройшла валідація успішно
     */
    private boolean valid;

    /**
     * Чи потрібне підтвердження користувача
     */
    private boolean requiresApproval;

    /**
     * Загальна кількість рядків в поточних даних
     */
    private int totalRows;

    /**
     * Кількість нових рядків (відсутні в попередньому snapshot)
     */
    private int newRows;

    /**
     * Кількість змінених рядків
     */
    private int modifiedRows;

    /**
     * Кількість видалених рядків (є в попередньому, відсутні в поточному)
     */
    private int deletedRows;

    /**
     * Кількість незмінених рядків
     */
    private int unchangedRows;

    /**
     * Список критичних помилок (блокують імпорт)
     */
    @Builder.Default
    private List<String> criticalErrors = new ArrayList<>();

    /**
     * Список попереджень (не блокують, але потребують уваги)
     */
    @Builder.Default
    private List<String> warnings = new ArrayList<>();

    /**
     * Колонки які зникли порівняно з mapping
     */
    @Builder.Default
    private List<String> missingColumns = new ArrayList<>();

    /**
     * Нові колонки які з'явились в Google Sheets
     */
    @Builder.Default
    private List<String> newColumns = new ArrayList<>();

    /**
     * Приклади змінених рядків (перші 5)
     * Map: row index -> Map<column, before/after>
     */
    @Builder.Default
    private List<RowChange> sampleChanges = new ArrayList<>();

    /**
     * Шлях до збереженого CSV файлу з поточними даними
     */
    private String csvPath;

    /**
     * Загальне повідомлення для користувача
     */
    private String summary;

    /**
     * Чи є це перший запуск задачі (немає попередніх даних)
     */
    private boolean firstRun;

    /**
     * Додати критичну помилку
     */
    public void addCriticalError(String error) {
        this.criticalErrors.add(error);
        this.valid = false;
    }

    /**
     * Додати попередження
     */
    public void addWarning(String warning) {
        this.warnings.add(warning);
        // Попередження вимагають approval
        this.requiresApproval = true;
    }

    /**
     * Чи є будь-які зміни
     */
    public boolean hasChanges() {
        return newRows > 0 || modifiedRows > 0 || deletedRows > 0 ||
                !missingColumns.isEmpty() || !newColumns.isEmpty();
    }
}
