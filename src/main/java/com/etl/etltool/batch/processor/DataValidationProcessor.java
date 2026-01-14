package com.etl.etltool.batch.processor;

import com.etl.etltool.dto.FieldMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@StepScope
public class DataValidationProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

    @Value("#{jobParameters['mapping']}")
    private String mappingJson;

    private List<FieldMap> mapping;
    private final Gson gson = new Gson();

    /**
     * ✅ Обробляємо кожен рядок:
     * 1. Валідація
     * 2. Трансформація згідно з mapping
     * 3. Фільтрація полів
     *
     * Повертає null якщо рядок треба пропустити
     */
    @Override
    public Map<String, Object> process(Map<String, Object> item) throws Exception {
        if (mapping == null) {
            mapping = parseMapping(mappingJson);
        }

        // Перевірка на порожній рядок
        boolean hasData = item.values().stream()
                .anyMatch(val -> val != null && !val.toString().trim().isEmpty());

        if (!hasData) {
            log.debug("Skipping empty row");
            return null; // Spring Batch пропустить цей рядок
        }

        // ✅ Трансформація: залишаємо тільки mapped поля
        Map<String, Object> processed = new LinkedHashMap<>();

        for (FieldMap field : mapping) {
            String sourceHeader = field.getSourceHeader();
            String targetColumn = field.getTargetColumn();

            Object value = item.get(sourceHeader);

            // Валідація та трансформація
            processed.put(targetColumn, sanitizeValue(value));
        }

        return processed;
    }

    /**
     * Очищення значень (можна додати свої правила)
     */
    private Object sanitizeValue(Object value) {
        if (value == null) {
            return null;
        }

        String strValue = value.toString().trim();

        if (strValue.isEmpty()) {
            return null;
        }

        // Приклад: видалення зайвих пробілів
        strValue = strValue.replaceAll("\\s+", " ");

        // Приклад: обрізання дуже довгих значень
        if (strValue.length() > 5000) {
            log.warn("Value too long, truncating: {}", strValue.substring(0, 50));
            return strValue.substring(0, 5000);
        }

        return strValue;
    }

    /**
     * Парсинг JSON mapping
     */
    private List<FieldMap> parseMapping(String json) {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("Mapping JSON is empty");
        }

        try {
            List<FieldMap> result = gson.fromJson(json, new TypeToken<List<FieldMap>>() {}.getType());

            if (result == null || result.isEmpty()) {
                throw new IllegalArgumentException("Mapping is empty after parsing");
            }

            log.info("Loaded mapping for {} fields", result.size());
            return result;

        } catch (Exception e) {
            log.error("Failed to parse mapping JSON: {}", json, e);
            throw new RuntimeException("Invalid mapping JSON: " + e.getMessage(), e);
        }
    }
}