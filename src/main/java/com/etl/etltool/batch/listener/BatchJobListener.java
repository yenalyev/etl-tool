package com.etl.etltool.batch.listener;

import com.etl.etltool.core.service.TaskExecutionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

/**
 * ✅ Інтеграція Spring Batch з SSE моніторингом
 * Слухає події Job (start, complete, fail)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchJobListener implements JobExecutionListener {

    private final TaskExecutionManager executionManager;

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
        BatchStatus status = jobExecution.getStatus();

        log.info("🏁 Batch job completed for task: {} with status: {}", taskId, status);

        if (status == BatchStatus.COMPLETED) {
            // Успішне завершення
            long writeCount = jobExecution.getStepExecutions().stream()
                    .mapToLong(step -> step.getWriteCount()) // ✅ mapToLong замість mapToInt
                    .sum();

            long readCount = jobExecution.getStepExecutions().stream()
                    .mapToLong(step -> step.getReadCount()) // ✅ mapToLong
                    .sum();

            long skipCount = jobExecution.getStepExecutions().stream()
                    .mapToLong(step -> step.getSkipCount()) // ✅ mapToLong
                    .sum();

            // Для updateProgress можна кастувати до int (якщо метод очікує int)
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

        } else if (status == BatchStatus.FAILED) {
            // Помилка
            String errorMessage = jobExecution.getAllFailureExceptions().stream()
                    .map(Throwable::getMessage)
                    .collect(Collectors.joining("; "));

            executionManager.log(taskId, "❌ Помилка виконання: " + errorMessage);
            executionManager.finish(taskId, false, "Помилка: " + errorMessage);

        } else {
            // Інші статуси (STOPPED, ABANDONED)
            executionManager.log(taskId, "⚠️ Job завершено зі статусом: " + status);
            executionManager.finish(taskId, false, "Job status: " + status);
        }
    }
}