package com.etl.etltool.core.service;

import com.etl.etltool.core.entity.SyncTask;
import com.etl.etltool.core.entity.TaskExecutionHistory;
import com.etl.etltool.core.repository.TaskExecutionHistoryRepository;
import com.etl.etltool.dto.TaskExecutionStats;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Сервіс для роботи з історією виконань задач
 *
 * Функціонал:
 * - Створення записів історії
 * - Оновлення статусу виконань
 * - Отримання статистики по задачах
 * - Автоматичне очищення старих записів
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskExecutionHistoryService {

    private final TaskExecutionHistoryRepository historyRepository;

    /**
     * Почати новий запис історії для задачі
     */
    @Transactional
    public TaskExecutionHistory startExecution(
            SyncTask task,
            String csvFilePath,
            Long csvFileSize,
            Boolean changesDetected,
            String changesDetails) {

        TaskExecutionHistory history = TaskExecutionHistory.builder()
                .task(task)
                .startTime(LocalDateTime.now())
                .status("RUNNING")
                .csvFilePath(csvFilePath)
                .csvFileSize(csvFileSize)
                .changesDetected(changesDetected)
                .changesDetails(changesDetails)
                .build();

        TaskExecutionHistory saved = historyRepository.save(history);

        log.info("Started execution history: ID={}, Task={}", saved.getId(), task.getId());

        return saved;
    }

    /**
     * Оновити історію після завершення виконання
     */
    @Transactional
    public void finishExecution(
            Long historyId,
            String status,
            Long rowsRead,
            Long rowsWritten,
            Long rowsSkipped,
            String errorMessage,
            Long batchJobExecutionId) {

        Optional<TaskExecutionHistory> historyOpt = historyRepository.findById(historyId);

        if (historyOpt.isEmpty()) {
            log.warn("History not found: ID={}", historyId);
            return;
        }

        TaskExecutionHistory history = historyOpt.get();
        history.setEndTime(LocalDateTime.now());
        history.setStatus(status);
        history.setRowsRead(rowsRead);
        history.setRowsWritten(rowsWritten);
        history.setRowsSkipped(rowsSkipped);
        history.setErrorMessage(errorMessage);
        history.setBatchJobExecutionId(batchJobExecutionId);

        historyRepository.save(history);

        log.info("Finished execution history: ID={}, Status={}, Duration={}ms",
                history.getId(), status, history.getDurationMs());
    }

    /**
     * Отримати історію для задачі
     */
    public List<TaskExecutionHistory> getTaskHistory(Long taskId) {
        return historyRepository.findByTaskIdOrderByStartTimeDesc(taskId);
    }

    /**
     * Отримати N останніх виконань для задачі
     */
    public List<TaskExecutionHistory> getRecentTaskHistory(Long taskId, int limit) {
        return historyRepository.findTopNByTaskId(taskId, PageRequest.of(0, limit));
    }

    /**
     * Отримати останнє успішне виконання
     */
    public Optional<TaskExecutionHistory> getLastSuccessfulExecution(Long taskId) {
        return historyRepository.findFirstByTaskIdAndStatusOrderByStartTimeDesc(
                taskId, "SUCCESS");
    }

    /**
     * Отримати статистику по задачі
     */
    public TaskExecutionStats getStats(Long taskId) {
        long successCount = historyRepository.countSuccessfulExecutions(taskId);
        long failedCount = historyRepository.countFailedExecutions(taskId);

        Optional<TaskExecutionHistory> lastExecution =
                historyRepository.findTopNByTaskId(taskId, PageRequest.of(0, 1))
                        .stream().findFirst();

        return TaskExecutionStats.builder()
                .taskId(taskId)
                .totalExecutions(successCount + failedCount)
                .successfulExecutions(successCount)
                .failedExecutions(failedCount)
                .lastExecutionTime(lastExecution.map(TaskExecutionHistory::getStartTime).orElse(null))
                .lastExecutionStatus(lastExecution.map(TaskExecutionHistory::getStatus).orElse(null))
                .build();
    }

    /**
     * Видалити стару історію (старшу за N днів)
     */
    @Transactional
    public void cleanupOldHistory(int daysToKeep) {
        LocalDateTime threshold = LocalDateTime.now().minusDays(daysToKeep);

        log.info("Cleaning up history older than: {}", threshold);

        historyRepository.deleteByStartTimeBefore(threshold);

        log.info("History cleanup completed");
    }
}