package com.scheduler.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.scheduler.Task;
import com.scheduler.TaskScheduler;
import com.scheduler.TaskState;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class TaskController {

    private static final Logger log = LoggerFactory.getLogger(TaskController.class);
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public void registerRoutes(Javalin app, TaskScheduler scheduler) {
        app.get("/api/tasks", ctx -> listTasks(ctx, scheduler));
        app.get("/api/tasks/{id}", ctx -> getTask(ctx, scheduler));
        app.post("/api/tasks", ctx -> createTask(ctx, scheduler));
        app.put("/api/tasks/{id}", ctx -> updateTask(ctx, scheduler));
        app.delete("/api/tasks/{id}", ctx -> deleteTask(ctx, scheduler));
        app.post("/api/tasks/{id}/cancel", ctx -> cancelTask(ctx, scheduler));
        app.get("/api/tasks/state/{state}", ctx -> getTasksByState(ctx, scheduler));
        app.get("/api/tasks/group/{group}", ctx -> getTasksByGroup(ctx, scheduler));
        app.get("/api/scheduler/snapshot", ctx -> getSnapshot(ctx, scheduler));
        app.get("/api/scheduler/health", ctx -> healthCheck(ctx, scheduler));
    }

    private void listTasks(Context ctx, TaskScheduler scheduler) {
        String group = ctx.queryParam("group");
        List<Task> tasks = group != null ? scheduler.getTasksByGroup(group) : scheduler.getAllTasks();
        ctx.json(tasks.stream().map(this::toDto).toList());
    }

    private void getTask(Context ctx, TaskScheduler scheduler) {
        String id = ctx.pathParam("id");
        scheduler.getTask(id)
                .ifPresentOrElse(
                        task -> ctx.json(toDto(task)),
                        () -> ctx.status(404).json(Map.of("error", "Task not found: " + id))
                );
    }

    private void createTask(Context ctx, TaskScheduler scheduler) {
        try {
            TaskDto dto = objectMapper.readValue(ctx.body(), TaskDto.class);
            Task task = fromDto(dto);
            String taskId = scheduler.submitTask(task);
            ctx.status(201).json(Map.of("id", taskId, "message", "Task created"));
        } catch (Exception e) {
            log.error("Failed to create task", e);
            ctx.status(400).json(Map.of("error", e.getMessage()));
        }
    }

    private void updateTask(Context ctx, TaskScheduler scheduler) {
        try {
            String id = ctx.pathParam("id");
            TaskDto dto = objectMapper.readValue(ctx.body(), TaskDto.class);
            scheduler.getTask(id).ifPresentOrElse(existing -> {
                Task updated = fromDto(dto);
                scheduler.deleteTask(id);
                scheduler.submitTask(updated);
                ctx.json(Map.of("message", "Task updated"));
            }, () -> ctx.status(404).json(Map.of("error", "Task not found: " + id)));
        } catch (Exception e) {
            ctx.status(400).json(Map.of("error", e.getMessage()));
        }
    }

    private void deleteTask(Context ctx, TaskScheduler scheduler) {
        String id = ctx.pathParam("id");
        try {
            scheduler.deleteTask(id);
            ctx.json(Map.of("message", "Task deleted"));
        } catch (Exception e) {
            ctx.status(404).json(Map.of("error", e.getMessage()));
        }
    }

    private void cancelTask(Context ctx, TaskScheduler scheduler) {
        String id = ctx.pathParam("id");
        boolean cancelled = scheduler.cancelTask(id);
        if (cancelled) {
            ctx.json(Map.of("message", "Task cancelled"));
        } else {
            ctx.status(404).json(Map.of("error", "Task not found or already terminal: " + id));
        }
    }

    private void getTasksByState(Context ctx, TaskScheduler scheduler) {
        String stateStr = ctx.pathParam("state");
        try {
            TaskState state = TaskState.valueOf(stateStr.toUpperCase());
            ctx.json(scheduler.getTasksByState(state).stream().map(this::toDto).toList());
        } catch (IllegalArgumentException e) {
            ctx.status(400).json(Map.of("error", "Invalid state: " + stateStr));
        }
    }

    private void getTasksByGroup(Context ctx, TaskScheduler scheduler) {
        String group = ctx.pathParam("group");
        ctx.json(scheduler.getTasksByGroup(group).stream().map(this::toDto).toList());
    }

    private void getSnapshot(Context ctx, TaskScheduler scheduler) {
        ctx.json(scheduler.getSnapshot());
    }

    private void healthCheck(Context ctx, TaskScheduler scheduler) {
        TaskScheduler.Snapshot snapshot = scheduler.getSnapshot();
        Map<String, Object> health = new LinkedHashMap<>();
        health.put("status", snapshot.isRunning() ? "UP" : "DOWN");
        health.put("nodeId", snapshot.nodeId());
        health.put("isLeader", snapshot.isLeader());
        health.put("totalTasks", snapshot.totalTasks());
        health.put("runningTasks", snapshot.runningTasks());
        health.put("queuedTasks", snapshot.queuedTasks());
        ctx.json(health);
    }

    @SuppressWarnings("unchecked")
    private TaskDto toDto(Task task) {
        TaskDto dto = new TaskDto();
        dto.id = task.getId();
        dto.name = task.getName();
        dto.type = task.getType();
        dto.payload = new HashMap<>(task.getPayload());
        dto.dependencies = new ArrayList<>(task.getDependencies());
        dto.priority = task.getPriority();
        dto.maxRetries = task.getMaxRetries();
        dto.timeoutSeconds = task.getTimeout() != null ? (int) task.getTimeout().getSeconds() : null;
        dto.cronExpression = task.getCronExpression();
        dto.group = task.getGroup();
        dto.state = task.getState().name();
        dto.retryCount = task.getRetryCount();
        dto.nextFireTime = task.getNextFireTime() != null ? task.getNextFireTime().toString() : null;
        dto.lastExecutedAt = task.getLastExecutedAt() != null ? task.getLastExecutedAt().toString() : null;
        dto.createdAt = task.getCreatedAt().toString();
        dto.updatedAt = task.getUpdatedAt().toString();
        dto.assignedNode = task.getAssignedNode();
        dto.errorMessage = task.getErrorMessage();
        return dto;
    }

    @SuppressWarnings("unchecked")
    private Task fromDto(TaskDto dto) {
        Task.Builder builder = Task.builder()
                .name(dto.name)
                .type(dto.type)
                .payload(dto.payload != null ? dto.payload : Map.of())
                .dependencies(dto.dependencies != null ? new LinkedHashSet<>(dto.dependencies) : new LinkedHashSet<>())
                .priority(dto.priority != null ? dto.priority : 0)
                .maxRetries(dto.maxRetries != null ? dto.maxRetries : 3)
                .group(dto.group != null ? dto.group : "default");

        if (dto.id != null) builder.id(dto.id);
        if (dto.timeoutSeconds != null) builder.timeout(Duration.ofSeconds(dto.timeoutSeconds));
        if (dto.cronExpression != null) builder.cronExpression(dto.cronExpression);
        if (dto.retryPolicy != null) {
            builder.retryPolicy(switch (dto.retryPolicy) {
                case "exponential" -> com.scheduler.retry.RetryPolicy.exponentialBackoff(
                        Duration.ofSeconds(1), Duration.ofMinutes(5), 2.0);
                default -> com.scheduler.retry.RetryPolicy.fixedDelay(Duration.ofSeconds(1));
            });
        }

        return builder.build();
    }

    public static class TaskDto {
        public String id;
        public String name;
        public String type;
        public Map<String, String> payload;
        public List<String> dependencies;
        public Integer priority;
        public Integer maxRetries;
        public String retryPolicy;
        public Integer timeoutSeconds;
        public String cronExpression;
        public String group;
        public String state;
        public Integer retryCount;
        public String nextFireTime;
        public String lastExecutedAt;
        public String createdAt;
        public String updatedAt;
        public String assignedNode;
        public String errorMessage;
    }
}
