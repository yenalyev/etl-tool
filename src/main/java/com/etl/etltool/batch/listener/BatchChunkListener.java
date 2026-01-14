package com.etl.etltool.batch.listener;

import com.etl.etltool.core.service.TaskExecutionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

/**
 * ✅ Моніторинг прогресу по chunk (кожні 1000 рядків)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchChunkListener implements ChunkListener {

    private final TaskExecutionManager executionManager;

    @Override
    public void beforeChunk(ChunkContext context) {
        // Нічого не робимо перед chunk
    }

    @Override
    public void afterChunk(ChunkContext context) {
        Long taskId = context.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getJobParameters()
                .getLong("taskId");

        long writeCount = context.getStepContext().getStepExecution().getWriteCount();
        long readCount = context.getStepContext().getStepExecution().getReadCount();

        // ✅ Оновлюємо прогрес в SSE
        executionManager.updateProgress(taskId, writeCount);

        // Логуємо кожні 5000 рядків (кожні 5 chunks)
        if (writeCount % 5000 == 0 && writeCount > 0) {
            executionManager.log(taskId,
                    String.format("📊 Прогрес: прочитано %d, записано %d рядків", readCount, writeCount));
        }
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        Long taskId = context.getStepContext()
                .getStepExecution()
                .getJobExecution()
                .getJobParameters()
                .getLong("taskId");

        executionManager.log(taskId, "⚠️ Помилка обробки chunk");
        log.error("Chunk processing error for task: {}", taskId);
    }
}