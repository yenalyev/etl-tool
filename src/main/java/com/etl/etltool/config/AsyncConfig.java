package com.etl.etltool.config;

import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Configuration
@EnableAsync
@EnableScheduling
public class AsyncConfig implements AsyncConfigurer {

    /**
     * ✅ Virtual Threads Executor для @Async методів
     */
    @Bean(name = "etlTaskExecutor")
    @Override
    public Executor getAsyncExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    /**
     * ✅ Глобальний обробник необроблених винятків в @Async методах
     */
    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return (ex, method, params) -> {
            System.err.println("==============================================");
            System.err.println("❌ Async Exception in: " + method.getName());
            System.err.println("Parameters: " + Arrays.toString(params));
            System.err.println("Exception: " + ex.getMessage());
            System.err.println("==============================================");
            ex.printStackTrace();
        };
    }
}