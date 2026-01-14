package com.etl.etltool.core.service;

import com.etl.etltool.dto.FieldMap;
import com.etl.etltool.model.SheetData;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DatabaseService {

    private final Gson gson = new Gson();

    /**
     * ОНОВЛЕНО: Приймає один об'єкт SheetData замість списку.
     */
    public void saveData(SheetData data, String tableName, String mappingJson, boolean createTable,
                         String url, String user, String password, Integer chunkSize) {

        if (data == null || data.getRows() == null || data.getRows().isEmpty()) {
            log.warn("Спроба зберегти порожній список даних.");
            return;
        }

        List<FieldMap> mapping = parseMapping(mappingJson);
        if (mapping.isEmpty()) {
            throw new RuntimeException("Мапінг полів не задано або він некоректний.");
        }

        int batchSize = (chunkSize != null && chunkSize > 0) ? chunkSize : 1000;

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            conn.setAutoCommit(false);

            if (createTable) {
                createTableIfNotExists(conn, tableName, mapping);
            }

            String insertSql = buildInsertSql(tableName, mapping);

            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                int count = 0;

                // --- ЗМІНА: Ітеруємось по getRows() з вашого класу ---
                for (Map<String, Object> row : data.getRows()) {

                    for (int i = 0; i < mapping.size(); i++) {
                        FieldMap field = mapping.get(i);
                        // Отримуємо значення з Map
                        Object valObj = row.get(field.getSourceHeader());
                        String val = valObj != null ? valObj.toString() : null;

                        if (val == null) {
                            ps.setNull(i + 1, Types.VARCHAR);
                        } else {
                            ps.setString(i + 1, val);
                        }
                    }

                    ps.addBatch();

                    if (++count % batchSize == 0) {
                        ps.executeBatch();
                        conn.commit();
                    }
                }

                ps.executeBatch();
                conn.commit();
                log.info("Successfully inserted total {} rows into '{}'", count, tableName);
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }

        } catch (Exception e) {
            log.error("Error saving data to database: {}", e.getMessage());
            throw new RuntimeException("Помилка запису в БД: " + e.getMessage(), e);
        }
    }

    public List<String> getColumnNames(String url, String user, String password, String tableName) {
        List<String> columns = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
                while (rs.next()) columns.add(rs.getString("COLUMN_NAME"));
            }
            if (columns.isEmpty()) {
                try (ResultSet rs = metaData.getColumns(null, null, tableName.toLowerCase(), null)) {
                    while (rs.next()) columns.add(rs.getString("COLUMN_NAME"));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Не вдалося отримати колонки таблиці: " + e.getMessage());
        }
        return columns;
    }

    private List<FieldMap> parseMapping(String json) {
        if (json == null || json.trim().isEmpty()) return new ArrayList<>();
        try {
            return gson.fromJson(json, new TypeToken<List<FieldMap>>(){}.getType());
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    private void createTableIfNotExists(Connection conn, String tableName, List<FieldMap> mapping) throws SQLException {
        String columnsDef = mapping.stream()
                .map(m -> m.getTargetColumn() + " TEXT")
                .collect(Collectors.joining(", "));
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, columnsDef);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private String buildInsertSql(String tableName, List<FieldMap> mapping) {
        String columns = mapping.stream()
                .map(FieldMap::getTargetColumn)
                .collect(Collectors.joining(", "));
        String placeholders = mapping.stream()
                .map(m -> "?")
                .collect(Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }
}