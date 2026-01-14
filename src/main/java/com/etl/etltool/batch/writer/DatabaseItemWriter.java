package com.etl.etltool.batch.writer;

import com.etl.etltool.config.DataSourceManager;
import com.etl.etltool.dto.FieldMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ✅ Thread-safe Writer для Spring Batch
 * - Використовує singleton DataSource (без витоку з'єднань)
 * - TRUNCATE таблиці перед першим chunk
 * - Детальне логування кожного кроку
 */
@Slf4j
@Component
@StepScope
@RequiredArgsConstructor
public class DatabaseItemWriter implements ItemWriter<Map<String, Object>> {

    private final DataSourceManager dataSourceManager;

    @Value("#{jobParameters['tableName']}")
    private String tableName;

    @Value("#{jobParameters['mapping']}")
    private String mappingJson;

    @Value("#{jobParameters['createTable']}")
    private String createTableStr;

    @Value("#{jobParameters['dbUrl']}")
    private String dbUrl;

    @Value("#{jobParameters['dbUser']}")
    private String dbUser;

    @Value("#{jobParameters['dbPassword']}")
    private String dbPassword;

    // Внутрішній стан
    private DataSource dataSource;
    private List<FieldMap> mapping;
    private boolean tableCreated = false;
    private boolean tableCleared = false;
    private int totalWritten = 0;
    private final Gson gson = new Gson();

    /**
     * ✅ Головний метод: Spring Batch викликає для кожного chunk (1000 рядків)
     */
    @Override
    public void write(Chunk<? extends Map<String, Object>> chunk) throws Exception {
        String threadName = Thread.currentThread().getName();
        int chunkSize = chunk.size();

        log.info("═══════════════════════════════════════════════════════");
        log.info("🔵 write() START - Thread: {}, Chunk size: {}", threadName, chunkSize);
        log.info("═══════════════════════════════════════════════════════");

        // ══════════════════════════════════════════════════════
        // КРОК 1: Ініціалізація DataSource (один раз)
        // ══════════════════════════════════════════════════════
        if (dataSource == null) {
            log.info("🔧 Initializing DataSource...");
            dataSource = dataSourceManager.getDataSource(dbUrl, dbUser, dbPassword);
            log.info("✅ DataSource initialized for: {}", dbUrl);
        }

        // ══════════════════════════════════════════════════════
        // КРОК 2: Парсинг mapping (один раз)
        // ══════════════════════════════════════════════════════
        if (mapping == null) {
            log.info("🔧 Parsing field mapping...");
            mapping = parseMapping(mappingJson);
            log.info("✅ Mapping parsed: {} fields", mapping.size());
        }

        // ══════════════════════════════════════════════════════
        // КРОК 3: Створення таблиці (якщо потрібно, один раз)
        // ══════════════════════════════════════════════════════
        if (Boolean.parseBoolean(createTableStr) && !tableCreated) {
            log.info("🔧 Creating table if not exists...");
            createTableIfNeeded();
            tableCreated = true;
        }

        // ══════════════════════════════════════════════════════
        // КРОК 4: TRUNCATE таблиці перед ПЕРШИМ chunk
        // ══════════════════════════════════════════════════════
        if (!tableCleared) {
            log.info("🗑️ First chunk detected - clearing table before insert");
            clearTableBeforeInsert();
            tableCleared = true;
        }

        // ══════════════════════════════════════════════════════
        // КРОК 5: Вставка даних
        // ══════════════════════════════════════════════════════
        log.info("💾 Inserting chunk data...");
        insertBatch(chunk.getItems());
        totalWritten += chunkSize;

        log.info("═══════════════════════════════════════════════════════");
        log.info("🔵 write() END - Written this chunk: {}, Total written: {}", chunkSize, totalWritten);
        log.info("═══════════════════════════════════════════════════════\n");
    }

    /**
     * ✅ TRUNCATE таблиці перед імпортом
     */
    private void clearTableBeforeInsert() throws SQLException {
        log.info("┌─────────────────────────────────────────────────");
        log.info("│ 🗑️  CLEARING TABLE: {}", tableName);
        log.info("└─────────────────────────────────────────────────");

        long startTime = System.currentTimeMillis();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            // Спроба 1: TRUNCATE (найшвидший)
            try {
                String truncateSql = String.format("TRUNCATE TABLE %s", tableName);
                log.info("📝 Executing: {}", truncateSql);
                stmt.execute(truncateSql);

                long duration = System.currentTimeMillis() - startTime;
                log.info("✅ TRUNCATE completed in {}ms", duration);

            } catch (SQLException e) {
                // Спроба 2: DELETE (якщо TRUNCATE не підтримується)
                log.warn("⚠️ TRUNCATE failed: {} - Trying DELETE...", e.getMessage());

                String deleteSql = String.format("DELETE FROM %s", tableName);
                log.info("📝 Executing: {}", deleteSql);
                stmt.execute(deleteSql);
                conn.commit();

                long duration = System.currentTimeMillis() - startTime;
                log.info("✅ DELETE completed in {}ms", duration);
            }

        } catch (SQLException e) {
            log.error("❌ Failed to clear table: {}", tableName, e);
            throw e;
        }
    }

    /**
     * ✅ Створення таблиці (якщо не існує)
     */
    private void createTableIfNeeded() throws SQLException {
        log.info("┌─────────────────────────────────────────────────");
        log.info("│ 📋 CREATING TABLE: {}", tableName);
        log.info("└─────────────────────────────────────────────────");

        String columnsDef = mapping.stream()
                .map(m -> m.getTargetColumn() + " TEXT")
                .collect(Collectors.joining(", "));

        String sql = String.format("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, columnsDef);
        log.info("📝 SQL: {}", sql);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(sql);
            log.info("✅ Table ready: {}", tableName);

        } catch (SQLException e) {
            log.error("❌ Failed to create table: {}", tableName, e);
            throw e;
        }
    }

    /**
     * ✅ Batch вставка даних
     */
    private void insertBatch(List<? extends Map<String, Object>> items) throws SQLException {
        log.info("┌─────────────────────────────────────────────────");
        log.info("│ 💾 BATCH INSERT: {} rows", items.size());
        log.info("└─────────────────────────────────────────────────");

        if (items.isEmpty()) {
            log.warn("⚠️ insertBatch() called with EMPTY items list - skipping");
            return;
        }

        long startTime = System.currentTimeMillis();
        String sql = buildInsertSql();
        log.info("📝 SQL: {}", sql);

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            log.info("🔧 AutoCommit disabled");

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                int batchCount = 0;

                // Додаємо кожен рядок у batch
                for (Map<String, Object> row : items) {
                    setParameters(ps, row);
                    ps.addBatch();
                    batchCount++;

                    // Логуємо кожен 100-й рядок для діагностики
                    if (batchCount % 100 == 0) {
                        log.debug("📊 Added {} statements to batch...", batchCount);
                    }
                }

                log.info("📊 Total statements in batch: {}", batchCount);

                // Виконуємо batch
                log.info("⚙️ Executing batch...");
                int[] results = ps.executeBatch();
                log.info("✅ executeBatch() returned {} results", results.length);

                // Перевірка результатів
                int successCount = 0;
                int failedCount = 0;
                for (int result : results) {
                    if (result == Statement.SUCCESS_NO_INFO || result >= 0) {
                        successCount++;
                    } else {
                        failedCount++;
                    }
                }

                log.info("📊 Batch results: {} success, {} failed", successCount, failedCount);

                // Commit транзакції
                log.info("⚙️ Committing transaction...");
                conn.commit();

                long duration = System.currentTimeMillis() - startTime;
                log.info("✅ Transaction committed in {}ms", duration);
                log.info("✅ Successfully inserted {} rows", items.size());

            } catch (SQLException e) {
                log.error("❌ Batch insert failed - rolling back transaction");
                conn.rollback();
                log.error("🔄 Rollback completed");
                throw e;
            }

        } catch (SQLException e) {
            log.error("❌ Database connection error during batch insert", e);
            throw e;
        }
    }

    /**
     * Побудова INSERT SQL
     */
    private String buildInsertSql() {
        String columns = mapping.stream()
                .map(FieldMap::getTargetColumn)
                .collect(Collectors.joining(", "));

        String placeholders = mapping.stream()
                .map(m -> "?")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    /**
     * Встановлення параметрів PreparedStatement
     */
    private void setParameters(PreparedStatement ps, Map<String, Object> row) throws SQLException {
        for (int i = 0; i < mapping.size(); i++) {
            FieldMap field = mapping.get(i);
            Object value = row.get(field.getTargetColumn());

            if (value == null) {
                ps.setNull(i + 1, Types.VARCHAR);
            } else {
                ps.setString(i + 1, value.toString());
            }
        }
    }

    /**
     * Парсинг JSON mapping
     */
    private List<FieldMap> parseMapping(String json) {
        log.info("🔧 Parsing mapping JSON...");

        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("Mapping JSON is empty");
        }

        try {
            List<FieldMap> result = gson.fromJson(json, new TypeToken<List<FieldMap>>() {}.getType());

            if (result == null || result.isEmpty()) {
                throw new IllegalArgumentException("Mapping is empty after parsing");
            }

            log.info("✅ Mapping parsed successfully:");
            for (int i = 0; i < result.size(); i++) {
                FieldMap field = result.get(i);
                log.info("   {}. {} → {}", i + 1, field.getSourceHeader(), field.getTargetColumn());
            }

            return result;

        } catch (Exception e) {
            log.error("❌ Failed to parse mapping JSON: {}", json, e);
            throw new RuntimeException("Invalid mapping JSON: " + e.getMessage(), e);
        }
    }
}