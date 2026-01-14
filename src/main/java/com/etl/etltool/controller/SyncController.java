package com.etl.etltool.controller;

import com.etl.etltool.core.service.BatchSyncService;
import com.etl.etltool.core.service.TaskExecutionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/sync")
@RequiredArgsConstructor
public class SyncController {

    private final BatchSyncService batchSyncService; // ✅ Замінено на Batch
    private final TaskExecutionManager executionManager;

    /**
     * Запуск ВСІХ активних задач
     */
    @PostMapping("/run")
    public ResponseEntity<?> runAllSyncs() {
        log.info("📢 API: Starting all active tasks...");
        batchSyncService.runAllActiveTasks();
        return ResponseEntity.ok(Map.of("message", "All active tasks started"));
    }

    /**
     * Запуск ОДНІЄЇ задачі
     */
    @PostMapping("/start/{taskId}")
    public ResponseEntity<?> startTask(@PathVariable Long taskId) {
        log.info("📢 API: Start task request for ID: {}", taskId);

        try {
            // Перевірка чи задача вже не виконується
            if (!executionManager.initTask(taskId)) {
                return ResponseEntity.status(409).body(
                        Map.of("error", "Task is already running")
                );
            }

            // Запускаємо асинхронно через Spring Batch
            batchSyncService.runTaskAsync(taskId);

            return ResponseEntity.ok(Map.of(
                    "message", "Task started",
                    "taskId", taskId
            ));

        } catch (Exception e) {
            log.error("❌ Failed to start task: {}", taskId, e);
            return ResponseEntity.status(500).body(
                    Map.of("error", e.getMessage())
            );
        }
    }

    /**
     * SSE Stream для real-time логів
     * ✅ Залишається без змін - працює з TaskExecutionManager
     */
    @GetMapping(value = "/stream/{taskId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamLogs(@PathVariable Long taskId) {
        log.info("📡 SSE: Client connected for task: {}", taskId);
        return executionManager.subscribe(taskId);
    }
}