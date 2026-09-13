package com.scheduler;

import com.scheduler.retry.RetryPolicy;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Task {

    private final String id;
    private final String name;
    private final String type;
    private final Map<String, String> payload;
    private final Set<String> dependencies;
    private final int priority;
    private final int maxRetries;
    private final RetryPolicy retryPolicy;
    private final Duration timeout;
    private final String cronExpression;
    private final String group;

    private volatile TaskState state;
    private volatile int retryCount;
    private volatile Instant nextFireTime;
    private volatile Instant lastExecutedAt;
    private volatile Instant createdAt;
    private volatile Instant updatedAt;
    private volatile String assignedNode;
    private volatile String errorMessage;

    private Task(Builder builder) {
        this.id = builder.id;
        this.name = builder.name;
        this.type = builder.type;
        this.payload = Map.copyOf(builder.payload);
        this.dependencies = new LinkedHashSet<>(builder.dependencies);
        this.priority = builder.priority;
        this.maxRetries = builder.maxRetries;
        this.retryPolicy = builder.retryPolicy;
        this.timeout = builder.timeout;
        this.cronExpression = builder.cronExpression;
        this.group = builder.group;
        this.state = TaskState.PENDING;
        this.retryCount = 0;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public String getId() { return id; }
    public String getName() { return name; }
    public String getType() { return type; }
    public Map<String, String> getPayload() { return payload; }
    public Set<String> getDependencies() { return Collections.unmodifiableSet(dependencies); }
    public int getPriority() { return priority; }
    public int getMaxRetries() { return maxRetries; }
    public RetryPolicy getRetryPolicy() { return retryPolicy; }
    public Duration getTimeout() { return timeout; }
    public String getCronExpression() { return cronExpression; }
    public String getGroup() { return group; }
    public TaskState getState() { return state; }
    public int getRetryCount() { return retryCount; }
    public Instant getNextFireTime() { return nextFireTime; }
    public Instant getLastExecutedAt() { return lastExecutedAt; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public String getAssignedNode() { return assignedNode; }
    public String getErrorMessage() { return errorMessage; }
    public boolean hasCronSchedule() { return cronExpression != null && !cronExpression.isBlank(); }

    public void setState(TaskState state) {
        assert this.state.canTransitionTo(state);
        this.state = state;
        this.updatedAt = Instant.now();
    }

    public void incrementRetryCount() { this.retryCount++; this.updatedAt = Instant.now(); }
    public void setNextFireTime(Instant nextFireTime) { this.nextFireTime = nextFireTime; this.updatedAt = Instant.now(); }
    public void setLastExecutedAt(Instant lastExecutedAt) { this.lastExecutedAt = lastExecutedAt; this.updatedAt = Instant.now(); }
    public void setAssignedNode(String assignedNode) { this.assignedNode = assignedNode; this.updatedAt = Instant.now(); }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; this.updatedAt = Instant.now(); }

    public boolean hasDependencies() { return !dependencies.isEmpty(); }
    public boolean canRetry() { return retryCount < maxRetries; }
    public boolean isExpired() { return timeout != null && lastExecutedAt != null && Instant.now().isAfter(lastExecutedAt.plus(timeout)); }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id = UUID.randomUUID().toString();
        private String name;
        private String type;
        private Map<String, String> payload = new HashMap<>();
        private Set<String> dependencies = new LinkedHashSet<>();
        private int priority = 0;
        private int maxRetries = 3;
        private RetryPolicy retryPolicy = RetryPolicy.fixedDelay(Duration.ofSeconds(1));
        private Duration timeout = Duration.ofMinutes(5);
        private String cronExpression;
        private String group = "default";

        public Builder id(String id) { this.id = id; return this; }
        public Builder name(String name) { this.name = name; return this; }
        public Builder type(String type) { this.type = type; return this; }
        public Builder payload(Map<String, String> payload) { this.payload = new HashMap<>(payload); return this; }
        public Builder addPayload(String key, String value) { this.payload.put(key, value); return this; }
        public Builder dependencies(Set<String> dependencies) { this.dependencies = new LinkedHashSet<>(dependencies); return this; }
        public Builder addDependency(String taskId) { this.dependencies.add(taskId); return this; }
        public Builder priority(int priority) { this.priority = priority; return this; }
        public Builder maxRetries(int maxRetries) { this.maxRetries = maxRetries; return this; }
        public Builder retryPolicy(RetryPolicy retryPolicy) { this.retryPolicy = retryPolicy; return this; }
        public Builder timeout(Duration timeout) { this.timeout = timeout; return this; }
        public Builder cronExpression(String cronExpression) { this.cronExpression = cronExpression; return this; }
        public Builder group(String group) { this.group = group; return this; }

        public Task build() {
            Objects.requireNonNull(name, "Task name is required");
            Objects.requireNonNull(type, "Task type is required");
            return new Task(this);
        }
    }
}
