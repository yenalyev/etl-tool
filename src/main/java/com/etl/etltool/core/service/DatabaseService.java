package com.etl.etltool.core.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class DatabaseService {

    public List<String> getColumnNames(String url, String user, String password, String tableName) {
        List<String> columns = new ArrayList<>();

        // Використовуємо try-with-resources для автоматичного закриття з'єднання
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            DatabaseMetaData metaData = conn.getMetaData();

            // Отримуємо колонки для конкретної таблиці
            // Параметри: catalog, schemaPattern, tableNamePattern, columnNamePattern
            try (ResultSet rs = metaData.getColumns(null, null, tableName, null)) {
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME"));
                }
            }

            // Якщо список порожній, можливо, справа в регістрі назви (PostgreSQL чутливий до цього)
            if (columns.isEmpty()) {
                try (ResultSet rs = metaData.getColumns(null, null, tableName.toLowerCase(), null)) {
                    while (rs.next()) {
                        columns.add(rs.getString("COLUMN_NAME"));
                    }
                }
            }

        } catch (SQLException e) {
            log.error("Помилка отримання метаданих БД: {}", e.getMessage());
            throw new RuntimeException("Не вдалося отримати колонки таблиці: " + e.getMessage());
        }

        return columns;
    }
}
