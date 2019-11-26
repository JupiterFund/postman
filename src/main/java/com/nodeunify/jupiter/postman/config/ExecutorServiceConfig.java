package com.nodeunify.jupiter.postman.config;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@EnableAsync
@Configuration
public class ExecutorServiceConfig {

    @Bean
    @ConfigurationProperties(prefix = "app.connect.executor-service")
    public ExecutorServiceProperties executorServiceProperties() {
        return new ExecutorServiceProperties();
    }

    @Data
    private static class ExecutorServiceProperties {
        private String type = "fixed";
        private int threadsNumber = 2;
    }

    private static class DelegatedExecutorService extends AbstractExecutorService {
        private final ExecutorService e;

        DelegatedExecutorService(ExecutorService executor) {
            e = executor;
        }

        public void execute(Runnable command) {
            e.execute(command);
        }

        public void shutdown() {
            e.shutdown();
        }

        public List<Runnable> shutdownNow() {
            return e.shutdownNow();
        }

        public boolean isShutdown() {
            return e.isShutdown();
        }

        public boolean isTerminated() {
            return e.isTerminated();
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return e.awaitTermination(timeout, unit);
        }

        public void destroy() {
            shutdown();
            try {
                // Wait a while for existing tasks to terminate
                if (!awaitTermination(60, TimeUnit.SECONDS)) {
                    shutdownNow(); // Cancel currently executing tasks
                    // Wait a while for tasks to respond to being cancelled
                    if (!awaitTermination(60, TimeUnit.SECONDS)) {
                        log.error("ExecutorService did not terminate");
                    }
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
        }
    }

    @Bean(destroyMethod = "destroy")
    public ExecutorService executorService(
            @Qualifier("executorServiceProperties") ExecutorServiceProperties properties) {
        ExecutorService executorService;
        switch (properties.getType().toUpperCase()) {
        case "SINGLE":
            executorService = Executors.newSingleThreadExecutor();
            break;
        case "CACHED":
            executorService = Executors.newCachedThreadPool();
            break;
        case "FIXED":
        default:
            executorService = Executors.newFixedThreadPool(properties.getThreadsNumber());
            break;
        }
        return new DelegatedExecutorService(executorService);
    }

}
