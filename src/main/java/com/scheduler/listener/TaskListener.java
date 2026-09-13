package com.scheduler.listener;

import com.scheduler.Task;
import com.scheduler.TaskState;

public interface TaskListener {

    void onTaskCreated(Task task);

    void onTaskStateChanged(Task task, TaskState oldState, TaskState newState);

    void onTaskExecutionStarted(Task task);

    void onTaskExecutionCompleted(Task task);

    void onTaskExecutionFailed(Task task, Throwable error);

    void onTaskRetrying(Task task, int attemptNumber, long delayMillis);

    default TaskListener andThen(TaskListener other) {
        return new TaskListener() {
            @Override
            public void onTaskCreated(Task task) {
                TaskListener.this.onTaskCreated(task);
                other.onTaskCreated(task);
            }

            @Override
            public void onTaskStateChanged(Task task, TaskState oldState, TaskState newState) {
                TaskListener.this.onTaskStateChanged(task, oldState, newState);
                other.onTaskStateChanged(task, oldState, newState);
            }

            @Override
            public void onTaskExecutionStarted(Task task) {
                TaskListener.this.onTaskExecutionStarted(task);
                other.onTaskExecutionStarted(task);
            }

            @Override
            public void onTaskExecutionCompleted(Task task) {
                TaskListener.this.onTaskExecutionCompleted(task);
                other.onTaskExecutionCompleted(task);
            }

            @Override
            public void onTaskExecutionFailed(Task task, Throwable error) {
                TaskListener.this.onTaskExecutionFailed(task, error);
                other.onTaskExecutionFailed(task, error);
            }

            @Override
            public void onTaskRetrying(Task task, int attemptNumber, long delayMillis) {
                TaskListener.this.onTaskRetrying(task, attemptNumber, delayMillis);
                other.onTaskRetrying(task, attemptNumber, delayMillis);
            }
        };
    }
}
