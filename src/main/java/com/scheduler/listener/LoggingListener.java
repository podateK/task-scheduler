package com.scheduler.listener;

import com.scheduler.Task;
import com.scheduler.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingListener implements TaskListener {

    private static final Logger log = LoggerFactory.getLogger(LoggingListener.class);

    @Override
    public void onTaskCreated(Task task) {
        log.info("Task created: id={}, name={}, type={}, group={}", task.getId(), task.getName(), task.getType(), task.getGroup());
    }

    @Override
    public void onTaskStateChanged(Task task, TaskState oldState, TaskState newState) {
        log.info("Task state changed: id={}, {} -> {}", task.getId(), oldState, newState);
    }

    @Override
    public void onTaskExecutionStarted(Task task) {
        log.info("Task execution started: id={}, attempt={}", task.getId(), task.getRetryCount() + 1);
    }

    @Override
    public void onTaskExecutionCompleted(Task task) {
        log.info("Task execution completed: id={}", task.getId());
    }

    @Override
    public void onTaskExecutionFailed(Task task, Throwable error) {
        log.error("Task execution failed: id={}, error={}", task.getId(), error.getMessage(), error);
    }

    @Override
    public void onTaskRetrying(Task task, int attemptNumber, long delayMillis) {
        log.warn("Task retrying: id={}, attempt={}, delayMs={}", task.getId(), attemptNumber, delayMillis);
    }
}
