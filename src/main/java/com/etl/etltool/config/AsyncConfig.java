package com.etl.etltool.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AsyncConfig {

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
}