package com.etl.etltool.core.service;

import com.etl.etltool.core.entity.AppConfig;
import com.etl.etltool.core.service.google.GoogleSheetsService;
import com.etl.etltool.dto.FieldMap;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class SyncService {

    private final GoogleSheetsService googleSheetsService;
    private final ObjectMapper objectMapper; // Додаємо Jackson для JSON

    public int runSync(AppConfig config) throws Exception {
        // 1. EXTRACT
        List<List<Object>> rawData = googleSheetsService.readSheet(config);
        if (rawData == null || rawData.isEmpty()) {
            throw new RuntimeException("Google Sheet порожній або недоступний.");
        }

        // 2. SETUP
        DataSource dataSource = createDataSource(config);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        String tableName = config.getTargetTableName();

        // Розбираємо JSON мапінг
        List<FieldMap> mappings = objectMapper.readValue(
                config.getFieldMappingJson(),
                new TypeReference<List<FieldMap>>() {}
        );

        // 3. PREPARE TABLE (якщо режим створення)
        if (config.isCreateNewTable()) {
            prepareTargetTable(jdbcTemplate, tableName, mappings);
        }

        // 4. LOAD
        jdbcTemplate.execute("DELETE FROM " + tableName);

        // Створюємо мапу: заголовок -> індекс у списку Google Sheets
        List<Object> headers = rawData.get(0);
        Map<String, Integer> headerIndexMap = new HashMap<>();
        for (int i = 0; i < headers.size(); i++) {
            headerIndexMap.put(headers.get(i).toString(), i);
        }

        // Фільтруємо мапінг: залишаємо тільки ті поля, які є в Google Sheets
        List<FieldMap> activeMappings = mappings.stream()
                .filter(m -> m.getTargetColumn() != null && !m.getTargetColumn().isEmpty())
                .filter(m -> headerIndexMap.containsKey(m.getSourceHeader()))
                .collect(Collectors.toList());

        String sql = generateInsertSql(tableName, activeMappings);
        List<List<Object>> dataRows = rawData.subList(1, rawData.size());

        for (List<Object> row : dataRows) {
            Object[] params = new Object[activeMappings.size()];
            for (int i = 0; i < activeMappings.size(); i++) {
                FieldMap m = activeMappings.get(i);
                int sourceIdx = headerIndexMap.get(m.getSourceHeader());
                params[i] = (sourceIdx < row.size()) ? row.get(sourceIdx) : null;
            }
            jdbcTemplate.update(sql, params);
        }

        return dataRows.size();
    }

    private void prepareTargetTable(JdbcTemplate jdbcTemplate, String tableName, List<FieldMap> mappings) {
        String columns = mappings.stream()
                .filter(m -> m.getTargetColumn() != null && !m.getTargetColumn().isEmpty())
                .map(m -> m.getTargetColumn() + " TEXT")
                .collect(Collectors.joining(", "));

        String sql = String.format("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, columns);
        log.info("Executing Table Creation: {}", sql);
        jdbcTemplate.execute(sql);
    }

    private String generateInsertSql(String tableName, List<FieldMap> mappings) {
        String columns = mappings.stream()
                .map(FieldMap::getTargetColumn)
                .collect(Collectors.joining(", "));

        String placeholders = mappings.stream()
                .map(m -> "?")
                .collect(Collectors.joining(", "));

        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columns, placeholders);
    }

    private DataSource createDataSource(AppConfig config) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setUrl(config.getTargetDbUrl());
        ds.setUsername(config.getTargetDbUser());
        ds.setPassword(config.getTargetDbPassword());

        // Авто-визначення драйвера
        String url = config.getTargetDbUrl().toLowerCase();
        if (url.contains("postgresql")) ds.setDriverClassName("org.postgresql.Driver");
        else if (url.contains("sqlite")) ds.setDriverClassName("org.sqlite.JDBC");

        return ds;
    }
}