package com.scheduler.metrics;

import io.micrometer.core.instrument.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SchedulerMetrics {

    private static final Logger log = LoggerFactory.getLogger(SchedulerMetrics.class);

    private final MeterRegistry registry;
    private final AtomicLong tasksScheduled = new AtomicLong(0);
    private final AtomicLong tasksCompleted = new AtomicLong(0);
    private final AtomicLong tasksFailed = new AtomicLong(0);
    private final AtomicLong tasksRetried = new AtomicLong(0);
    private final AtomicLong activeExecutions = new AtomicLong(0);
    private final ConcurrentHashMap<String, Timer.Sample> activeTimers = new ConcurrentHashMap<>();

    public SchedulerMetrics(MeterRegistry registry) {
        this.registry = registry;
        registerGauges();
    }

    public SchedulerMetrics() {
        this(Metrics.globalRegistry);
    }

    private void registerGauges() {
        Gauge.builder("scheduler.tasks.scheduled", tasksScheduled, AtomicLong::doubleValue)
                .description("Total tasks scheduled")
                .register(registry);
        Gauge.builder("scheduler.tasks.completed", tasksCompleted, AtomicLong::doubleValue)
                .description("Total tasks completed")
                .register(registry);
        Gauge.builder("scheduler.tasks.failed", tasksFailed, AtomicLong::doubleValue)
                .description("Total tasks failed")
                .register(registry);
        Gauge.builder("scheduler.tasks.retried", tasksRetried, AtomicLong::doubleValue)
                .description("Total tasks retried")
                .register(registry);
        Gauge.builder("scheduler.executions.active", activeExecutions, AtomicLong::doubleValue)
                .description("Currently active executions")
                .register(registry);
    }

    public void recordTaskScheduled(String taskType, String group) {
        tasksScheduled.incrementAndGet();
        Counter.builder("scheduler.tasks.total")
                .tag("state", "scheduled")
                .tag("type", taskType)
                .tag("group", group)
                .register(registry)
                .increment();
    }

    public void recordTaskCompleted(String taskType, String group, long durationNanos) {
        tasksCompleted.incrementAndGet();
        Timer.builder("scheduler.task.execution")
                .tag("state", "completed")
                .tag("type", taskType)
                .tag("group", group)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordTaskFailed(String taskType, String group, long durationNanos, String errorType) {
        tasksFailed.incrementAndGet();
        Timer.builder("scheduler.task.execution")
                .tag("state", "failed")
                .tag("type", taskType)
                .tag("group", group)
                .tag("error", errorType)
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public void recordTaskRetried(String taskType, String group) {
        tasksRetried.incrementAndGet();
        Counter.builder("scheduler.tasks.total")
                .tag("state", "retried")
                .tag("type", taskType)
                .tag("group", group)
                .register(registry)
                .increment();
    }

    public void startExecution(String taskId) {
        activeExecutions.incrementAndGet();
        activeTimers.put(taskId, Timer.start(registry));
    }

    public void endExecution(String taskId, String taskType, String group, boolean success) {
        activeExecutions.decrementAndGet();
        Timer.Sample sample = activeTimers.remove(taskId);
        if (sample != null) {
            long duration = sample.stop(Timer.builder("scheduler.task.duration")
                    .tag("type", taskType)
                    .tag("group", group)
                    .tag("success", String.valueOf(success))
                    .register(registry));
            if (success) {
                recordTaskCompleted(taskType, group, duration);
            } else {
                recordTaskFailed(taskType, group, duration, "execution_error");
            }
        }
    }

    public void recordQueueSize(long size) {
        registry.gauge("scheduler.queue.size", size);
    }

    public void recordStoreOperation(String operation, long durationNanos) {
        Timer.builder("scheduler.store.operation")
                .tag("operation", operation)
                .publishPercentiles(0.5, 0.95)
                .register(registry)
                .record(durationNanos, TimeUnit.NANOSECONDS);
    }

    public long getTasksScheduled() { return tasksScheduled.get(); }
    public long getTasksCompleted() { return tasksCompleted.get(); }
    public long getTasksFailed() { return tasksFailed.get(); }
    public long getTasksRetried() { return tasksRetried.get(); }
    public long getActiveExecutions() { return activeExecutions.get(); }
}
