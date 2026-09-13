package com.scheduler;

public enum TaskState {
    PENDING,
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED,
    RETRYING;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == CANCELLED;
    }

    public boolean canTransitionTo(TaskState target) {
        return switch (this) {
            case PENDING -> target == QUEUED || target == CANCELLED;
            case QUEUED -> target == RUNNING || target == CANCELLED;
            case RUNNING -> target == COMPLETED || target == FAILED || target == RETRYING;
            case RETRYING -> target == QUEUED || target == CANCELLED;
            case COMPLETED, FAILED, CANCELLED -> false;
        };
    }
}
