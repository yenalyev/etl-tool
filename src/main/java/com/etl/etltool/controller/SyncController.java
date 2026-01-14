package com.etl.etltool.controller;

import com.etl.etltool.core.service.SyncService;
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
@RequestMapping("/api/sync") // Базовий шлях
@RequiredArgsConstructor
public class SyncController {

    private final SyncService syncService;
    private final TaskExecutionManager executionManager;

    // Запуск ВСІХ задач
    @PostMapping("/run")
    public ResponseEntity<?> runAllSyncs() {
        log.info("Запуск всіх задач...");
        syncService.runAllActiveTasks();
        return ResponseEntity.ok(Map.of("message", "All tasks started async"));
    }

    // Запуск ОДНІЄЇ задачі
    @PostMapping("/run/{taskId}")
    public ResponseEntity<?> runTaskApi(@PathVariable Long taskId) {
        log.info("Запуск задачі ID: {}", taskId);
        // 1. Ініціалізуємо статус в менеджері
        executionManager.initTask(taskId);
        // 2. Запускаємо асинхронно (метод повертає управління миттєво)
        syncService.runTaskAsync(taskId);

        return ResponseEntity.ok(Map.of("message", "Started", "taskId", taskId));
    }


    // Метод нічого не повертає по суті (void логіка), тільки "ОК"
    @PostMapping("/start/{taskId}")
    public ResponseEntity<?> startTask(@PathVariable Long taskId) {
        try {
            // Перевіряємо, чи задача вже не виконується
            if (!executionManager.initTask(taskId)) {
                return ResponseEntity.status(409).body(
                        Map.of("error", "Task is already running")
                );
            }

            syncService.runTaskAsync(taskId);
            return ResponseEntity.ok(Map.of("message", "Task started", "taskId", taskId));
        } catch (Exception e) {
            log.error("Failed to start task", e);
            return ResponseEntity.status(500).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping(value = "/stream/{taskId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamLogs(@PathVariable Long taskId) {
        return executionManager.subscribe(taskId);
    }
}