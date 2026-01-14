package com.etl.etltool.core.entity;

import jakarta.persistence.*;
import lombok.*;

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

    private String googleSheetId;
    private String serviceAccountKeyPath;

    private String targetDbUrl;
    private String targetDbUser;
    private String targetDbPassword;

    private Integer defaultChunkSize;
    private String cronExpression;

    private boolean autoStartEnabled;

    private String targetTableName;

    @Column(name = "create_new_table", nullable = false, columnDefinition = "boolean default false")
    private boolean createNewTable = false;

    @Column(columnDefinition = "TEXT")
    private String fieldMappingJson; // JSON представлення списку FieldMap

    @Column(name = "sheet_name")
    private String sheetName;
}