package com.etl.etltool.core.service;

import com.etl.etltool.core.entity.SyncTask;
import com.etl.etltool.dto.FieldMap;
import com.etl.etltool.dto.RowChange;
import com.etl.etltool.dto.ValidationResult;
import com.etl.etltool.dto.csv.CsvData;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Сервіс для валідації даних та виявлення змін (ETL Tool)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DataValidationService {

    private final CsvStorageService csvStorageService;
    private final Gson gson = new Gson();

    /**
     * Головний метод валідації
     */
    public ValidationResult validateData(
            SyncTask task,
            List<String> currentHeaders,
            List<List<Object>> currentRows) {

        log.info("🔍 Starting validation for task: {}. Rows: {}", task.getId(), currentRows.size());

        try {
            // 1. Валідація структури
            ValidationResult structureResult = validateStructure(task, currentHeaders);
            if (!structureResult.isValid()) {
                log.error("❌ Structure validation failed");
                return structureResult;
            }

            // 2. Завантаження попередніх даних
            Optional<CsvData> previousDataOpt = csvStorageService.loadLatestCsv(task.getId());

            if (previousDataOpt.isEmpty()) {
                log.info("✅ First run detected");
                return buildFirstRunResult(currentRows.size(), structureResult);
            }

            // 3. Порівняння даних (Детекція змін)
            CsvData prevData = previousDataOpt.get();
            ValidationResult comparisonResult = compareData(
                    task,
                    currentHeaders, currentRows,
                    prevData.getHeaders(), prevData.getRows()
            );

            // Об'єднуємо попередження структури з результатом порівняння
            comparisonResult.getWarnings().addAll(structureResult.getWarnings());
            comparisonResult.getMissingColumns().addAll(structureResult.getMissingColumns());
            comparisonResult.getNewColumns().addAll(structureResult.getNewColumns());

            if (comparisonResult.hasChanges() || !structureResult.getWarnings().isEmpty()) {
                comparisonResult.setRequiresApproval(true);
            }

            return comparisonResult;

        } catch (Exception e) {
            log.error("❌ Validation crash: ", e);
            return ValidationResult.builder()
                    .valid(false)
                    .criticalErrors(List.of("System error: " + e.getMessage()))
                    .build();
        }
    }

    /**
     * Перевірка наявності необхідних колонок
     */
    private ValidationResult validateStructure(SyncTask task, List<String> currentHeaders) {
        ValidationResult result = new ValidationResult();
        result.setValid(true);

        List<FieldMap> mapping = parseMapping(task.getFieldMappingJson());
        if (mapping.isEmpty()) {
            result.addCriticalError("Field mapping is empty");
            return result;
        }

        List<String> expectedColumns = mapping.stream()
                .map(FieldMap::getSourceHeader)
                .collect(Collectors.toList());

        List<String> missing = expectedColumns.stream()
                .filter(col -> !currentHeaders.contains(col))
                .collect(Collectors.toList());

        List<String> newCols = currentHeaders.stream()
                .filter(col -> !expectedColumns.contains(col))
                .collect(Collectors.toList());

        if (!missing.isEmpty()) {
            result.addCriticalError("Missing required columns: " + String.join(", ", missing));
            result.setMissingColumns(missing);
        }

        if (!newCols.isEmpty()) {
            result.addWarning("New columns detected: " + String.join(", ", newCols));
            result.setNewColumns(newCols);
        }

        return result;
    }

    /**
     * Основна логіка порівняння рядків
     */
    private ValidationResult compareData(
            SyncTask task,
            List<String> currentHeaders, List<List<Object>> currentRows,
            List<String> previousHeaders, List<List<Object>> previousRows) {

        List<FieldMap> mapping = parseMapping(task.getFieldMappingJson());
        List<String> mappedColumns = mapping.stream()
                .map(FieldMap::getSourceHeader)
                .collect(Collectors.toList());

        // Оптимізація індексів
        Map<String, Integer> currentHeaderMap = createHeaderMap(currentHeaders);
        Map<String, Integer> prevHeaderMap = createHeaderMap(previousHeaders);

        int newRows = 0;
        int modifiedRows = 0;
        int unchangedRows = 0;
        List<RowChange> sampleChanges = new ArrayList<>();

        // Порівняння за позицією (index-based)
        for (int i = 0; i < currentRows.size(); i++) {
            List<Object> currentRow = currentRows.get(i);

            if (i >= previousRows.size()) {
                newRows++;
                if (sampleChanges.size() < 5) {
                    sampleChanges.add(createRowChange(i, "NEW", currentHeaders, currentRow, null, null, mappedColumns, currentHeaderMap, prevHeaderMap));
                }
                continue;
            }

            List<Object> prevRow = previousRows.get(i);
            String currentHash = generateRowHash(currentHeaders, currentRow, mappedColumns, currentHeaderMap);
            String prevHash = generateRowHash(previousHeaders, prevRow, mappedColumns, prevHeaderMap);

            if (currentHash.equals(prevHash)) {
                unchangedRows++;
            } else {
                modifiedRows++;
                if (sampleChanges.size() < 5) {
                    sampleChanges.add(createRowChange(i, "MODIFIED", currentHeaders, currentRow, previousHeaders, prevRow, mappedColumns, currentHeaderMap, prevHeaderMap));
                }
            }
        }

        int deletedRows = Math.max(0, previousRows.size() - currentRows.size());

        return ValidationResult.builder()
                .valid(true)
                .totalRows(currentRows.size())
                .newRows(newRows)
                .modifiedRows(modifiedRows)
                .deletedRows(deletedRows)
                .unchangedRows(unchangedRows)
                .sampleChanges(sampleChanges)
                .summary(buildComparisonSummary(currentRows.size(), newRows, modifiedRows, deletedRows, unchangedRows))
                .build();
    }

    private String generateRowHash(List<String> headers, List<Object> row, List<String> mappedColumns, Map<String, Integer> headerMap) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            StringBuilder sb = new StringBuilder();

            for (String col : mappedColumns) {
                Integer idx = headerMap.get(col);
                if (idx != null && idx < row.size()) {
                    sb.append(String.valueOf(row.get(idx))).append("|");
                }
            }
            byte[] hash = md.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Integer> createHeaderMap(List<String> headers) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < headers.size(); i++) map.put(headers.get(i), i);
        return map;
    }

    private RowChange createRowChange(int rowIndex, String type, List<String> curH, List<Object> curR, List<String> prevH, List<Object> prevR, List<String> mapped, Map<String, Integer> curM, Map<String, Integer> prevM) {
        Map<String, String> sample = new HashMap<>();
        for (int i = 0; i < Math.min(3, mapped.size()); i++) {
            String col = mapped.get(i);
            Integer idx = curM.get(col);
            if (idx != null && idx < curR.size()) sample.put(col, String.valueOf(curR.get(idx)));
        }

        Map<String, String[]> changes = new HashMap<>();
        if ("MODIFIED".equals(type) && prevR != null) {
            for (String col : mapped) {
                Integer cIdx = curM.get(col);
                Integer pIdx = prevM.get(col);
                String cVal = (cIdx != null && cIdx < curR.size()) ? String.valueOf(curR.get(cIdx)) : "";
                String pVal = (pIdx != null && pIdx < prevR.size()) ? String.valueOf(prevR.get(pIdx)) : "";
                if (!cVal.equals(pVal)) changes.put(col, new String[]{pVal, cVal});
            }
        }

        return RowChange.builder().rowIndex(rowIndex).changeType(type).sampleData(sample).columnChanges(changes).build();
    }

    private ValidationResult buildFirstRunResult(int total, ValidationResult struct) {
        return ValidationResult.builder()
                .valid(true).firstRun(true).totalRows(total).newRows(total)
                .requiresApproval(!struct.getWarnings().isEmpty())
                .warnings(struct.getWarnings())
                .summary("🆕 Перший імпорт: " + total + " рядків буде додано.")
                .build();
    }

    private String buildComparisonSummary(int total, int n, int m, int d, int u) {
        return String.format("📊 Результат: Всього %d | 🆕 Нових: %d | ✏️ Змінених: %d | 🗑️ Видалених: %d | ✅ Без змін: %d", total, n, m, d, u);
    }

    private List<FieldMap> parseMapping(String json) {
        if (json == null || json.isBlank()) return Collections.emptyList();
        try {
            return gson.fromJson(json, new TypeToken<List<FieldMap>>() {}.getType());
        } catch (Exception e) {
            log.error("Mapping error", e);
            return Collections.emptyList();
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}