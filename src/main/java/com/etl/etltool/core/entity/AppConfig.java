package com.etl.etltool.core.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "app_config")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AppConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // --- ГЛОБАЛЬНІ НАЛАШТУВАННЯ ---

    // Шлях до JSON ключа (припускаємо, що Service Account один на всіх)
    private String serviceAccountKeyPath;

    // Налаштування БД (одна цільова база)
    private String targetDbUrl;
    private String targetDbUser;
    private String targetDbPassword;

    // Глобальні налаштування планувальника
    private Integer defaultChunkSize;
    private String cronExpression; // Загальний розклад запуску
    private boolean autoStartEnabled;

    // --- ПОЛЯ НИЖЧЕ ВИДАЛЕНІ, БО ВОНИ ТЕПЕР У SyncTask ---
    // googleSheetId, sheetName, targetTableName, createNewTable, fieldMappingJson
}