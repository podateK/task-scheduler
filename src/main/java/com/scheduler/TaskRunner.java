package com.scheduler;

import com.scheduler.listener.TaskListener;
import com.scheduler.metrics.SchedulerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class TaskRunner {

    private static final Logger log = LoggerFactory.getLogger(TaskRunner.class);

    private final SchedulerConfig config;
    private final SchedulerMetrics metrics;
    private final ExecutorService executor;
    private final Map<String, Function<Task, String>> taskHandlers = new ConcurrentHashMap<>();
    private final TaskListener listener;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public TaskRunner(SchedulerConfig config, SchedulerMetrics metrics, TaskListener listener) {
        this.config = config;
        this.metrics = metrics;
        this.listener = listener;
        this.executor = new ThreadPoolExecutor(
                config.getCorePoolSize(),
                config.getMaxPoolSize(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> {
                    Thread t = new Thread(r, "task-runner-" + ThreadLocalRandom.current().nextInt(1000));
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    public void registerHandler(String taskType, Function<Task, String> handler) {
        taskHandlers.put(taskType, handler);
    }

    public CompletableFuture<TaskExecutionResult> execute(Task task) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("TaskRunner is shutting down"));
        }

        Function<Task, String> handler = taskHandlers.get(task.getType());
        if (handler == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "No handler registered for task type: " + task.getType()));
        }

        return CompletableFuture.supplyAsync(() -> {
            if (!running.get()) {
                throw new IllegalStateException("TaskRunner is shutting down");
            }
            return executeWithMetrics(task, handler);
        }, executor);
    }

    private TaskExecutionResult executeWithMetrics(Task task, Function<Task, String> handler) {
        Instant start = Instant.now();
        metrics.startExecution(task.getId());
        listener.onTaskExecutionStarted(task);

        try {
            Duration timeout = task.getTimeout() != null ? task.getTimeout() : config.getTaskTimeout();
            String result = callWithTimeout(handler, task, timeout);

            long durationNanos = Duration.between(start, Instant.now()).toNanos();
            metrics.endExecution(task.getId(), task.getType(), task.getGroup(), true);
            listener.onTaskExecutionCompleted(task);

            return new TaskExecutionResult(task.getId(), true, result, null, durationNanos);
        } catch (Exception e) {
            long durationNanos = Duration.between(start, Instant.now()).toNanos();
            metrics.endExecution(task.getId(), task.getType(), task.getGroup(), false);
            listener.onTaskExecutionFailed(task, e);

            return new TaskExecutionResult(task.getId(), false, null, e.getMessage(), durationNanos);
        }
    }

    private String callWithTimeout(Function<Task, String> handler, Task task, Duration timeout) {
        try {
            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> handler.apply(task), executor);
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException("Task execution timed out after " + timeout);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            throw new RuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Task execution interrupted", e);
        }
    }

    public void shutdown() {
        running.set(false);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(config.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
                log.warn("TaskRunner forced shutdown");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public int getActiveCount() {
        if (executor instanceof ThreadPoolExecutor tpe) {
            return tpe.getActiveCount();
        }
        return 0;
    }

    public int getQueueSize() {
        if (executor instanceof ThreadPoolExecutor tpe) {
            return tpe.getQueue().size();
        }
        return 0;
    }

    public record TaskExecutionResult(
            String taskId,
            boolean success,
            String result,
            String errorMessage,
            long durationNanos
    ) {}
}
