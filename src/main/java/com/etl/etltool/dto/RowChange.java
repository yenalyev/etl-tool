package com.etl.etltool.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Деталі окремої зміни в рядку
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RowChange {
    /**
     * Номер рядка (0-based)
     */
    private int rowIndex;

    /**
     * Тип зміни: NEW, MODIFIED, DELETED
     */
    private String changeType;

    /**
     * Зміни по колонках: column name -> [oldValue, newValue]
     */
    private Map<String, String[]> columnChanges;

    /**
     * Приклад даних (перші 3 колонки для попереду)
     */
    private Map<String, String> sampleData;
}
