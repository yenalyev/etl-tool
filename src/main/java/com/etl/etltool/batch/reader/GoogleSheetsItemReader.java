package com.etl.etltool.batch.reader;

import com.etl.etltool.core.service.google.GoogleSheetsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@StepScope // ✅ КРИТИЧНО: дозволяє використовувати JobParameters
public class GoogleSheetsItemReader implements ItemReader<Map<String, Object>> {

    private final GoogleSheetsService googleSheetsService;

    @Value("#{jobParameters['sheetId']}")
    private String sheetId;

    @Value("#{jobParameters['sheetName']}")
    private String sheetName;

    @Value("#{jobParameters['keyPath']}")
    private String keyPath;

    private Iterator<Map<String, Object>> rowIterator;
    private List<String> headers;
    private boolean initialized = false;
    private int totalRows = 0;

    @Autowired
    public GoogleSheetsItemReader(GoogleSheetsService googleSheetsService) {
        this.googleSheetsService = googleSheetsService;
    }

    /**
     * ✅ Spring Batch викликає цей метод для кожного рядка
     * Повертає null коли дані закінчились
     */
    @Override
    public Map<String, Object> read() throws Exception {
        if (!initialized) {
            initialize();
        }

        if (rowIterator != null && rowIterator.hasNext()) {
            return rowIterator.next();
        }

        // null = сигнал для Spring Batch що дані закінчились
        return null;
    }

    /**
     * Ініціалізація: читаємо дані з Google Sheets один раз
     */
    private void initialize() throws Exception {
        log.info("📥 Initializing Google Sheets reader for: {} (sheet: {})",
                sheetId, sheetName != null ? sheetName : "default");

        try {
            // Використовуємо існуючий сервіс з Semaphore
            List<List<Object>> rawData = googleSheetsService.readSheet(
                    sheetId,
                    sheetName,
                    keyPath
            );

            if (rawData == null || rawData.isEmpty()) {
                log.warn("⚠️ No data found in spreadsheet");
                this.rowIterator = Collections.emptyIterator();
                this.totalRows = 0;
                this.initialized = true;
                return;
            }

            // Перший рядок = заголовки
            this.headers = rawData.get(0).stream()
                    .map(obj -> obj != null ? obj.toString() : "")
                    .collect(Collectors.toList());

            // ✅ Створюємо Iterator (не копіюємо дані в нову структуру!)
            this.rowIterator = rawData.stream()
                    .skip(1) // Пропускаємо рядок заголовків
                    .map(this::convertRowToMap)
                    .iterator();

            this.totalRows = rawData.size() - 1;
            log.info("✅ Initialized reader. Total rows: {}", totalRows);

        } catch (Exception e) {
            log.error("❌ Failed to initialize Google Sheets reader", e);
            throw new RuntimeException("Failed to read Google Sheets: " + e.getMessage(), e);
        } finally {
            this.initialized = true;
        }
    }

    /**
     * Конвертує List<Object> в Map<String, Object>
     */
    private Map<String, Object> convertRowToMap(List<Object> row) {
        Map<String, Object> map = new LinkedHashMap<>();

        for (int i = 0; i < headers.size(); i++) {
            String header = headers.get(i);
            Object value = (i < row.size()) ? row.get(i) : null;

            // Нормалізація: пусті рядки → null
            if (value != null && value.toString().trim().isEmpty()) {
                value = null;
            }

            map.put(header, value);
        }

        return map;
    }

    /**
     * Getter для логування (скільки всього рядків)
     */
    public int getTotalRows() {
        return totalRows;
    }
}