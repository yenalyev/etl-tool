package com.etl.etltool.config;

import com.etl.etltool.core.service.TaskExecutionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Set;

/**
 * ✅ Graceful Shutdown для Batch Jobs
 * Зупиняє активні задачі ПЕРЕД закриттям connection pools
 */
@Slf4j
@Component
@Order(1) // ✅ Виконується ПЕРШИМ при shutdown
@RequiredArgsConstructor
public class BatchJobShutdownManager {

    private final JobOperator jobOperator;
    private final JobExplorer jobExplorer;
    private final TaskExecutionManager executionManager;

    @PreDestroy
    public void shutdownGracefully() {
        log.info("═══════════════════════════════════════════════════════");
        log.info("🛑 ПОЧАТОК GRACEFUL SHUTDOWN BATCH JOBS");
        log.info("═══════════════════════════════════════════════════════");

        try {
            // Отримуємо всі активні виконання для нашого job
            Set<Long> runningExecutions = jobOperator.getRunningExecutions("etlJob");

            if (runningExecutions.isEmpty()) {
                log.info("✅ Активних batch jobs не знайдено");
                return;
            }

            log.info("⏳ Знайдено {} активних batch job(s), зупиняємо...", runningExecutions.size());

            // Зупиняємо кожен активний job
            for (Long executionId : runningExecutions) {
                try {
                    log.info("🛑 Зупинка JobExecution: {}", executionId);

                    // Шукаємо taskId для логування
                    Long taskId = findTaskIdByExecutionId(executionId);
                    if (taskId != null) {
                        executionManager.log(taskId, "🛑 Система зупиняється - зупинка задачі...");
                    }

                    // Зупиняємо через JobOperator
                    jobOperator.stop(executionId);
                    log.info("✅ Надіслано команду зупинки для execution: {}", executionId);

                } catch (NoSuchJobExecutionException e) {
                    log.warn("⚠️ JobExecution {} вже не існує", executionId);
                } catch (Exception e) {
                    log.error("❌ Помилка зупинки execution {}: {}", executionId, e.getMessage());
                }
            }

            // Чекаємо завершення jobs (максимум 30 секунд)
            int maxWaitSeconds = 30;
            int waited = 0;

            while (!jobOperator.getRunningExecutions("etlJob").isEmpty() && waited < maxWaitSeconds) {
                Thread.sleep(1000);
                waited++;

                if (waited % 5 == 0) {
                    int remaining = jobOperator.getRunningExecutions("etlJob").size();
                    log.info("⏳ Очікування завершення... ({} секунд, залишилось jobs: {})",
                            waited, remaining);
                }
            }

            if (waited >= maxWaitSeconds) {
                log.warn("⚠️ Таймаут очікування зупинки jobs (30 сек), продовжуємо shutdown");
                log.warn("⚠️ Активних jobs: {}", jobOperator.getRunningExecutions("etlJob").size());
            } else {
                log.info("✅ Всі batch jobs успішно зупинені за {} секунд", waited);
            }

        } catch (Exception e) {
            log.error("❌ Критична помилка під час shutdown batch jobs", e);
        }

        log.info("✅ Graceful shutdown batch jobs завершено");
        log.info("═══════════════════════════════════════════════════════\n");
    }

    /**
     * Знаходить taskId за executionId для логування
     */
    private Long findTaskIdByExecutionId(Long executionId) {
        try {
            var jobExecution = jobExplorer.getJobExecution(executionId);
            if (jobExecution != null) {
                return jobExecution.getJobParameters().getLong("taskId");
            }
        } catch (Exception e) {
            log.debug("Не вдалося знайти taskId для execution: {}", executionId);
        }
        return null;
    }
}