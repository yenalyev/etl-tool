package com.etl.etltool.batch.listener;

import com.etl.etltool.core.service.TaskExecutionManager;
import com.etl.etltool.core.service.TaskExecutionHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

/**
 * ✅ ОНОВЛЕНО: Інтеграція з історією виконань
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchJobListener implements JobExecutionListener {

    private final TaskExecutionManager executionManager;
    private final TaskExecutionHistoryService historyService; // ✅ ДОДАНО

    @Override
    public void beforeJob(JobExecution jobExecution) {
        Long taskId = jobExecution.getJobParameters().getLong("taskId");

        log.info("🚀 Starting batch job for task: {}", taskId);
        executionManager.log(taskId, "🚀 Spring Batch job started");
        executionManager.log(taskId, "📊 Job ID: " + jobExecution.getJobId());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Long taskId = jobExecution.getJobParameters().getLong("taskId");
        Long historyId = jobExecution.getJobParameters().getLong("historyId"); // ✅ ДОДАНО
        BatchStatus status = jobExecution.getStatus();

        log.info("🏁 Batch job completed for task: {} with status: {}", taskId, status);

        if (status == BatchStatus.COMPLETED) {
            handleSuccess(taskId, historyId, jobExecution);

        } else if (status == BatchStatus.FAILED) {
            handleFailure(taskId, historyId, jobExecution);

        } else if (status == BatchStatus.STOPPED) {
            handleStopped(taskId, historyId, jobExecution);

        } else {
            handleOther(taskId, historyId, jobExecution, status);
        }
    }

    /**
     * ✅ НОВИЙ: Обробка успішного завершення
     */
    private void handleSuccess(Long taskId, Long historyId, JobExecution jobExecution) {
        long writeCount = jobExecution.getStepExecutions().stream()
                .mapToLong(step -> step.getWriteCount())
                .sum();

        long readCount = jobExecution.getStepExecutions().stream()
                .mapToLong(step -> step.getReadCount())
                .sum();

        long skipCount = jobExecution.getStepExecutions().stream()
                .mapToLong(step -> step.getSkipCount())
                .sum();

        executionManager.updateProgress(taskId, (int) writeCount);

        String message = String.format(
                "✅ Успішно завершено!\n" +
                        "📥 Прочитано: %d рядків\n" +
                        "💾 Записано: %d рядків\n" +
                        "⚠️ Пропущено: %d рядків",
                readCount, writeCount, skipCount
        );

        executionManager.log(taskId, message);
        executionManager.finish(taskId, true, "Імпорт завершено успішно");

        // ✅ ОНОВЛЮЄМО ІСТОРІЮ
        if (historyId != null) {
            historyService.finishExecution(
                    historyId,
                    "SUCCESS",
                    readCount,
                    writeCount,
                    skipCount,
                    null,
                    jobExecution.getJobId()
            );
            log.info("✅ History updated: ID={}, Status=SUCCESS", historyId);
        }
    }

    /**
     * ✅ НОВИЙ: Обробка помилки
     */
    private void handleFailure(Long taskId, Long historyId, JobExecution jobExecution) {
        String errorMessage = jobExecution.getAllFailureExceptions().stream()
                .map(Throwable::getMessage)
                .collect(Collectors.joining("; "));

        executionManager.log(taskId, "❌ Помилка виконання: " + errorMessage);
        executionManager.finish(taskId, false, "Помилка: " + errorMessage);

        // ✅ ОНОВЛЮЄМО ІСТОРІЮ
        if (historyId != null) {
            long readCount = jobExecution.getStepExecutions().stream()
                    .mapToLong(step -> step.getReadCount())
                    .sum();

            long writeCount = jobExecution.getStepExecutions().stream()
                    .mapToLong(step -> step.getWriteCount())
                    .sum();

            historyService.finishExecution(
                    historyId,
                    "FAILED",
                    readCount,
                    writeCount,
                    0L,
                    errorMessage,
                    jobExecution.getJobId()
            );
            log.info("✅ History updated: ID={}, Status=FAILED", historyId);
        }
    }

    /**
     * ✅ НОВИЙ: Обробка зупинки
     */
    private void handleStopped(Long taskId, Long historyId, JobExecution jobExecution) {
        log.info("🛑 Job was stopped for task: {}", taskId);

        long writeCount = jobExecution.getStepExecutions().stream()
                .mapToLong(step -> step.getWriteCount())
                .sum();

        long readCount = jobExecution.getStepExecutions().stream()
                .mapToLong(step -> step.getReadCount())
                .sum();

        executionManager.updateProgress(taskId, (int) writeCount);

        String message = String.format(
                "🛑 Задачу зупинено користувачем\n" +
                        "📊 Оброблено до зупинки:\n" +
                        "📥 Прочитано: %d рядків\n" +
                        "💾 Записано: %d рядків",
                readCount, writeCount
        );

        executionManager.log(taskId, message);
        executionManager.finish(taskId, false, "Зупинено користувачем");

        // ✅ ОНОВЛЮЄМО ІСТОРІЮ
        if (historyId != null) {
            historyService.finishExecution(
                    historyId,
                    "STOPPED",
                    readCount,
                    writeCount,
                    0L,
                    "Stopped by user",
                    jobExecution.getJobId()
            );
            log.info("✅ History updated: ID={}, Status=STOPPED", historyId);
        }
    }

    /**
     * Обробка інших статусів
     */
    private void handleOther(Long taskId, Long historyId, JobExecution jobExecution, BatchStatus status) {
        executionManager.log(taskId, "⚠️ Job завершено зі статусом: " + status);
        executionManager.finish(taskId, false, "Job status: " + status);

        // ✅ ОНОВЛЮЄМО ІСТОРІЮ
        if (historyId != null) {
            historyService.finishExecution(
                    historyId,
                    status.name(),
                    null, null, null,
                    "Job finished with status: " + status,
                    jobExecution.getJobId()
            );
            log.info("✅ History updated: ID={}, Status={}", historyId, status);
        }
    }
}