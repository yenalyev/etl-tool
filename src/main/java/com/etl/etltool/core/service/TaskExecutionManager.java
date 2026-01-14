package com.etl.etltool.core.service;

import com.etl.etltool.dto.ExecutionState;
import lombok.extern.slf4j.Slf4j;
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

    public void initTask(Long taskId) {
        ExecutionState state = new ExecutionState();
        state.setRunning(true);
        state.setStartTime(LocalDateTime.now());
        state.addLog("⏳ Задача поставлена в чергу...");
        taskStates.put(taskId, state);
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
        SseEmitter emitter = emitters.get(taskId);
        if (emitter != null) {
            try {
                emitter.send(SseEmitter.event().name(name).data(data));
            } catch (IOException e) {
                emitters.remove(taskId);
            }
        }
    }
}