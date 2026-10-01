package com.youssef.batch.config;

import org.springframework.boot.autoconfigure.batch.BatchTaskExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

@Configuration
public class AsyncConfig {

    /**
     * Runs each job launch on its own thread (batch-1, batch-2, ...).
     * @BatchTaskExecutor is the qualifier Spring Boot's DefaultBatchConfiguration
     * looks for — without it, this bean is ignored and the launcher stays synchronous.
     * SimpleAsyncTaskExecutor = one thread per launch; bounded with a concurrency
     * limit for safety; use a ThreadPoolTaskExecutor for production.
     */
    @Bean
    @BatchTaskExecutor
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor ste = new SimpleAsyncTaskExecutor("batch-");
        ste.setConcurrencyLimit(4);
        return ste;
    }
}