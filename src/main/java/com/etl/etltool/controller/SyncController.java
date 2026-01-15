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
     */
    @GetMapping(value = "/stream/{taskId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamLogs(@PathVariable Long taskId) {
        log.info("📡 SSE: Client connected for task: {}", taskId);
        return executionManager.subscribe(taskId);
    }


    /**
     * Зупинка задачі
     */
    @PostMapping("/stop/{taskId}")
    public ResponseEntity<?> stopTask(@PathVariable Long taskId) {
        log.info("📢 API: Stop task request for ID: {}", taskId);

        try {
            // Перевіряємо чи можна зупинити задачу
            if (!executionManager.canStopTask(taskId)) {
                return ResponseEntity.status(400).body(
                        Map.of(
                                "error", "Task cannot be stopped",
                                "reason", "Task is not running or already stopped"
                        )
                );
            }

            // Зупиняємо задачу
            boolean stopped = batchSyncService.stopTask(taskId);

            if (stopped) {
                return ResponseEntity.ok(Map.of(
                        "message", "Stop command sent successfully",
                        "taskId", taskId,
                        "note", "Task will stop after completing current chunk"
                ));
            } else {
                return ResponseEntity.status(500).body(
                        Map.of("error", "Failed to send stop command")
                );
            }

        } catch (Exception e) {
            log.error("❌ Failed to stop task: {}", taskId, e);
            return ResponseEntity.status(500).body(
                    Map.of("error", e.getMessage())
            );
        }
    }

    /**
     * Перевірка статусу задачі
     */
    @GetMapping("/status/{taskId}")
    public ResponseEntity<?> getTaskStatus(@PathVariable Long taskId) {
        try {
            var state = executionManager.getState(taskId);
            var jobExecution = executionManager.getJobExecution(taskId);

            if (state == null) {
                return ResponseEntity.status(404).body(
                        Map.of("error", "Task not found or not initialized")
                );
            }

            return ResponseEntity.ok(Map.of(
                    "taskId", taskId,
                    "running", state.isRunning(),
                    "finished", state.isFinished(),
                    "success", state.isSuccess(),
                    "rowsProcessed", state.getRowsProcessed(),
                    "canStop", jobExecution != null && jobExecution.isRunning(),
                    "jobStatus", jobExecution != null ? jobExecution.getStatus().name() : "UNKNOWN"
            ));

        } catch (Exception e) {
            log.error("❌ Failed to get task status: {}", taskId, e);
            return ResponseEntity.status(500).body(
                    Map.of("error", e.getMessage())
            );
        }
    }
}