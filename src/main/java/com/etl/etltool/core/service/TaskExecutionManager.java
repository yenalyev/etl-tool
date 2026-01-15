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
    private final Map<Long, org.springframework.batch.core.JobExecution> activeJobExecutions = new ConcurrentHashMap<>();


    /**
     * Підтримка повторних запусків
     *
     * Ініціалізація задачі перед запуском.
     * Повертає true якщо задачу можна запустити, false якщо вона вже виконується.
     */
    public boolean initTask(Long taskId) {
        // Перевіряємо поточний стан
        ExecutionState currentState = taskStates.get(taskId);

        // Якщо задача вже виконується - блокуємо запуск
        if (currentState != null && currentState.isRunning()) {
            log.warn("❌ Task {} is already running! Cannot start again.", taskId);
            return false;
        }

        // Створюємо новий стан (очищаємо старий, якщо був)
        ExecutionState newState = new ExecutionState();
        newState.setRunning(true);
        newState.setStartTime(LocalDateTime.now());
        newState.addLog("⏳ Задача поставлена в чергу...");

        // Використовуємо put() замість putIfAbsent() Це дозволяє замінити старий завершений стан новим
        taskStates.put(taskId, newState);

        log.info("✅ Task {} initialized successfully. Previous state: {}",
                taskId, currentState != null ? (currentState.isFinished() ? "finished" : "unknown") : "none");

        return true;
    }

    /**
     *  Закриття SSE з'єднання перед повторним запуском
     */
    public void closeEmitter(Long taskId) {
        SseEmitter emitter = emitters.remove(taskId);
        if (emitter != null) {
            try {
                emitter.complete();
                log.info("🔌 Closed SSE emitter for task: {}", taskId);
            } catch (Exception e) {
                log.debug("Failed to close emitter gracefully: {}", e.getMessage());
            }
        }
    }

    /**
     *  Отримання поточного стану (для діагностики)
     */
    public ExecutionState getState(Long taskId) {
        return taskStates.get(taskId);
    }

    public SseEmitter subscribe(Long taskId) {
        // Тайм-аут 1 година
        SseEmitter emitter = new SseEmitter(3600000L);

        //  ВИПРАВЛЕНО: Закриваємо старе з'єднання якщо є
        closeEmitter(taskId);

        // Додаємо нове
        emitters.put(taskId, emitter);

        Runnable cleanup = () -> {
            emitters.remove(taskId);
            log.debug("🧹 Cleaned up emitter for task: {}", taskId);
        };

        emitter.onCompletion(cleanup);
        emitter.onTimeout(cleanup);
        emitter.onError((e) -> {
            log.warn("⚠️ SSE error for task {}: {}", taskId, e.getMessage());
            cleanup.run();
        });

        // Відправляємо історію при підключенні (для відновлення сторінки)
        ExecutionState state = taskStates.get(taskId);
        if (state != null) {
            try {
                emitter.send(SseEmitter.event().name("history").data(state));
                log.debug("📜 Sent history to new subscriber for task: {}", taskId);
            } catch (IOException e) {
                log.warn("Failed to send history: {}", e.getMessage());
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
        } else {
            log.warn("⚠️ Attempted to log to non-existent task: {}", taskId);
        }
    }

    public void updateProgress(Long taskId, long count) {
        ExecutionState state = taskStates.get(taskId);
        if (state != null) {
            state.setRowsProcessed(count);
            sendEvent(taskId, "progress", count);
        }
    }

    private void sendEvent(Long taskId, String name, Object data) {
        // Атомарно отримуємо emitter
        SseEmitter emitter = emitters.get(taskId);
        if (emitter != null) {
            try {
                synchronized (emitter) { // Синхронізація на emitter
                    emitter.send(SseEmitter.event().name(name).data(data));
                }
            } catch (IOException | IllegalStateException e) {
                log.warn("Failed to send SSE event '{}' to task {}: {}", name, taskId, e.getMessage());
                emitters.remove(taskId);

                // Пробуємо закрити emitter коректно
                try {
                    emitter.completeWithError(e);
                } catch (Exception ignored) {}
            }
        }
    }

    /**
     * Автоматичне очищення старих станів (кожну годину)
     */
    @Scheduled(fixedDelay = 3600000)
    public void cleanupOldStates() {
        LocalDateTime threshold = LocalDateTime.now().minusHours(24);

        int removed = 0;
        for (Map.Entry<Long, ExecutionState> entry : taskStates.entrySet()) {
            ExecutionState state = entry.getValue();
            if (state.isFinished() &&
                    state.getEndTime() != null &&
                    state.getEndTime().isBefore(threshold)) {
                taskStates.remove(entry.getKey());
                removed++;
            }
        }

        if (removed > 0) {
            log.info("🧹 Cleaned up {} old task states. Remaining: {}", removed, taskStates.size());
        }
    }

    /**
     * Реєстрація JobExecution після запуску
     */
    public void registerJobExecution(Long taskId, org.springframework.batch.core.JobExecution jobExecution) {
        activeJobExecutions.put(taskId, jobExecution);
        log.info("✅ Registered JobExecution {} for task {}", jobExecution.getJobId(), taskId);
    }

    /**
     * Отримання JobExecution для зупинки
     */
    public org.springframework.batch.core.JobExecution getJobExecution(Long taskId) {
        return activeJobExecutions.get(taskId);
    }

    /**
     * Видалення JobExecution після завершення
     */
    public void removeJobExecution(Long taskId) {
        org.springframework.batch.core.JobExecution removed = activeJobExecutions.remove(taskId);
        if (removed != null) {
            log.info("🗑️ Removed JobExecution {} for task {}", removed.getJobId(), taskId);
        }
    }

    /**
     * Перевірка чи можна зупинити задачу
     */
    public boolean canStopTask(Long taskId) {
        ExecutionState state = taskStates.get(taskId);
        if (state == null || !state.isRunning()) {
            return false;
        }

        org.springframework.batch.core.JobExecution jobExecution = activeJobExecutions.get(taskId);
        return jobExecution != null && jobExecution.isRunning();
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

            log.info("🏁 Task {} finished: {}", taskId, success ? "SUCCESS" : "FAILED");
        }

        removeJobExecution(taskId);

        // Закриваємо з'єднання SSE коректно
        SseEmitter emitter = emitters.get(taskId);
        if (emitter != null) {
            try {
                Thread.sleep(500);
                emitter.complete();
            } catch (Exception ignored) {}
            emitters.remove(taskId);
        }
    }

    /**
     * Статистика для моніторингу
     */
    public Map<String, Object> getStatistics() {
        int total = taskStates.size();
        long running = taskStates.values().stream().filter(ExecutionState::isRunning).count();
        long finished = taskStates.values().stream().filter(ExecutionState::isFinished).count();

        return Map.of(
                "totalTasks", total,
                "runningTasks", running,
                "finishedTasks", finished,
                "activeEmitters", emitters.size(),
                "activeJobs", activeJobExecutions.size()
        );
    }
}