package com.etl.etltool.config;

import com.etl.etltool.core.service.ApplicationContextProvider;
import com.etl.etltool.core.service.TaskExecutionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Arrays;
import java.util.concurrent.Executor;

@Configuration
@EnableAsync
@EnableScheduling
@Slf4j
public class AsyncConfig implements AsyncConfigurer {

    @Bean(name = "etlTaskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);   // Одночасно працюють мінімум 5 задач
        executor.setMaxPoolSize(15);   // Максимум 15 при піковому навантаженні
        executor.setQueueCapacity(50); // Черга, якщо всі потоки зайняті
        executor.setThreadNamePrefix("EtlWorker-");
        executor.initialize();
        return executor;
    }

    // Глобальний обробник помилок для @Async
    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return (ex, method, params) -> {
            log.error("Async exception in method: {} with params: {}",
                    method.getName(), Arrays.toString(params), ex);

            // Якщо це метод runTaskAsync, спробуємо витягнути taskId
            if (params.length > 0 && params[0] instanceof Long) {
                Long taskId = (Long) params[0];
                // Повідомляємо через TaskExecutionManager
                try {
                    TaskExecutionManager manager =
                            ApplicationContextProvider.getBean(TaskExecutionManager.class);
                    manager.finish(taskId, false, "Критична помилка: " + ex.getMessage());
                } catch (Exception e) {
                    log.error("Failed to report async error", e);
                }
            }
        };
    }
}