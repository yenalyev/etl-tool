package com.etl.etltool.core.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "sync_tasks")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncTask {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String taskName; // Назва для зручності (напр. "Імпорт Товарів")

    private String googleSheetId;
    private String sheetName; // Якщо пусто - береться перша сторінка

    private String targetTableName;
    private boolean createNewTable; // Чи створювати таблицю, якщо її немає

    @Column(columnDefinition = "TEXT")
    private String fieldMappingJson; // JSON мапінгу для цієї конкретної задачі

    private boolean enabled; // Чи активна задача (для автозапуску)

    // окремий CRON для кожної задачі в майбутньому
    private String cronExpression;
}