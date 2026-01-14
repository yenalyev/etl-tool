package com.etl.etltool.core.service;

import com.etl.etltool.dto.ExecutionState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class TaskExecutionManager {

    private final Map<Long, ExecutionState> taskStates = new ConcurrentHashMap<>();
    private final Map<Long, SseEmitter> emitters = new ConcurrentHashMap<>();

    public boolean initTask(Long taskId) {
        ExecutionState newState = new ExecutionState();
        newState.setRunning(true);
        newState.setStartTime(LocalDateTime.now());
        newState.addLog("⏳ Задача поставлена в чергу...");

        // Атомарна операція: повертає попереднє значення
        ExecutionState prev = taskStates.putIfAbsent(taskId, newState);

        if (prev != null && prev.isRunning()) {
            log.warn("Task {} is already running!", taskId);
            return false; // Задача вже виконується
        }

        return true;
    }

    public SseEmitter subscribe(Long taskId) {
        // Тайм-аут 1 година
        SseEmitter emitter = new SseEmitter(3600000L);
        emitters.put(taskId, emitter);

        Runnable cleanup = () -> emitters.remove(taskId);
        emitter.onCompletion(cleanup);
        emitter.onTimeout(cleanup);
        emitter.onError((e) -> cleanup.run());

        // Відправляємо історію при підключенні (для відновлення сторінки)
        ExecutionState state = taskStates.get(taskId);
        if (state != null) {
            try {
                emitter.send(SseEmitter.event().name("history").data(state));
            } catch (IOException e) {
                emitters.remove(taskId);
            }
        }
        return emitter;
    }

    public void log(Long taskId, String message) {
        ExecutionState state = taskStates.get(taskId);
        if (state != null) {
            state.addLog(message);
            sendEvent(taskId, "log", message);
        }
    }

    public void updateProgress(Long taskId, int count) {
        ExecutionState state = taskStates.get(taskId);
        if (state != null) {
            state.setRowsProcessed(count);
            sendEvent(taskId, "progress", count);
        }
    }

    public void finish(Long taskId, boolean success, String message) {
        ExecutionState state = taskStates.get(taskId);
        if (state != null) {
            state.setRunning(false);
            state.setFinished(true);
            state.setSuccess(success);
            state.setEndTime(LocalDateTime.now());
            state.addLog((success ? "✅ " : "❌ ") + message);

            sendEvent(taskId, "log", (success ? "✅ " : "❌ ") + message);
            sendEvent(taskId, "finished", success);
        }
        // Закриваємо з'єднання SSE коректно
        SseEmitter emitter = emitters.get(taskId);
        if (emitter != null) {
            try { Thread.sleep(500); emitter.complete(); } catch (Exception ignored) {}
            emitters.remove(taskId);
        }
    }

    private void sendEvent(Long taskId, String name, Object data) {
        // ✅ Атомарно отримуємо і видаляємо при помилці
        SseEmitter emitter = emitters.get(taskId);
        if (emitter != null) {
            try {
                synchronized (emitter) { // Синхронізація на emitter
                    emitter.send(SseEmitter.event().name(name).data(data));
                }
            } catch (IOException | IllegalStateException e) {
                log.warn("Failed to send SSE event to task {}: {}", taskId, e.getMessage());
                emitters.remove(taskId);

                // Пробуємо закрити emitter коректно
                try {
                    emitter.completeWithError(e);
                } catch (Exception ignored) {}
            }
        }
    }

    // Автоматичне очищення старих станів
    @Scheduled(fixedDelay = 3600000) // Кожну годину
    public void cleanupOldStates() {
        LocalDateTime threshold = LocalDateTime.now().minusHours(24);

        taskStates.entrySet().removeIf(entry -> {
            ExecutionState state = entry.getValue();
            return state.isFinished() &&
                    state.getEndTime() != null &&
                    state.getEndTime().isBefore(threshold);
        });

        log.info("Cleaned up old task states. Remaining: {}", taskStates.size());
    }
}