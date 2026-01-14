package com.etl.etltool.core.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DatabaseConfigService {

    public Map<String, Object> testConnection(String url, String user, String password) {
        log.info("Тестування JDBC з'єднання для: {}", url);

        try {
            // ПРИМУСОВА РЕЄСТРАЦІЯ ДРАЙВЕРІВ
            if (url.startsWith("jdbc:postgresql:")) {
                Class.forName("org.postgresql.Driver");
            } else if (url.startsWith("jdbc:mysql:")) {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }

            // Встановлюємо таймаут 5 секунд
            DriverManager.setLoginTimeout(5);

            try (Connection conn = DriverManager.getConnection(url, user, password)) {
                if (conn != null && !conn.isClosed()) {
                    String databaseName = conn.getMetaData().getDatabaseProductName();
                    String version = conn.getMetaData().getDatabaseProductVersion();
                    return Map.of(
                            "success", true,
                            "message", "Успішно! Підключено до " + databaseName + " (" + version + ")"
                    );
                }
            }
        } catch (ClassNotFoundException e) {
            log.error("Драйвер не знайдено: {}", e.getMessage());
            return Map.of("success", false, "message", "Помилка: Драйвер для цієї БД не завантажено. Перевірте Maven.");
        } catch (SQLException e) {
            log.error("Помилка підключення до БД: {}", e.getMessage());
            return Map.of("success", false, "message", "Помилка JDBC: " + e.getMessage());
        }
        return Map.of("success", false, "message", "Невідома помилка підключення");
    }


    public List<String> getExistingTables(String url, String user, String password) {
        List<String> tables = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            // Отримуємо метадані бази даних
            DatabaseMetaData metaData = conn.getMetaData();
            // Витягуємо тільки "TABLE" (ігноруємо системні в’юхи)
            try (ResultSet rs = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            log.error("Помилка отримання таблиць: {}", e.getMessage());
        }
        return tables;
    }
}