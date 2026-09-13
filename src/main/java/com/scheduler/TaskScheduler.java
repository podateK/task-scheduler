package com.scheduler;

import com.scheduler.cluster.ClusterManager;
import com.scheduler.cluster.HeartbeatManager;
import com.scheduler.cron.CronExpression;
import com.scheduler.listener.LoggingListener;
import com.scheduler.listener.TaskListener;
import com.scheduler.metrics.SchedulerMetrics;
import com.scheduler.queue.PriorityTaskQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class TaskScheduler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TaskScheduler.class);

    private final SchedulerConfig config;
    private final TaskStore store;
    private final DependencyResolver dependencyResolver;
    private final PriorityTaskQueue pendingQueue;
    private final TaskRunner taskRunner;
    private final SchedulerMetrics metrics;
    private final ClusterManager clusterManager;
    private final HeartbeatManager heartbeatManager;
    private final TaskListener listener;

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, CompletableFuture<TaskRunner.TaskExecutionResult>> runningTasks = new ConcurrentHashMap<>();

    public TaskScheduler(SchedulerConfig config) {
        this(config, createDefaultStore(config), new LoggingListener());
    }

    public TaskScheduler(SchedulerConfig config, TaskStore store, TaskListener listener) {
        this.config = config;
        this.store = store;
        this.listener = listener;
        this.dependencyResolver = new DependencyResolver();
        this.pendingQueue = new PriorityTaskQueue();
        this.metrics = config.isMetricsEnabled() ? new SchedulerMetrics() : null;
        this.taskRunner = new TaskRunner(config, metrics, listener);
        this.clusterManager = new ClusterManager(config);
        this.heartbeatManager = new HeartbeatManager(config);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "task-scheduler-main");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            store.initialize();
            clusterManager.start();
            heartbeatManager.start();

            scheduler.scheduleWithFixedDelay(this::tick,
                    0, config.getScheduleInterval().toMillis(), TimeUnit.MILLISECONDS);

            log.info("TaskScheduler started with nodeId={}", config.getNodeId());
        }
    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            log.info("Shutting down TaskScheduler...");

            scheduler.shutdown();
            taskRunner.shutdown();
            heartbeatManager.stop();
            clusterManager.stop();

            try {
                if (!scheduler.awaitTermination(config.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

            cancelRunningTasks();
            store.close();
            log.info("TaskScheduler shutdown complete");
        }
    }

    public String submitTask(Task task) {
        Objects.requireNonNull(task, "Task cannot be null");
        validateTask(task);

        TaskState previousState = task.getState();
        if (task.hasDependencies()) {
            resolveDependencies(task);
        }
        task.setState(TaskState.QUEUED);

        if (task.hasCronSchedule()) {
            scheduleNextFire(task);
        }

        store.save(task);
        pendingQueue.offer(task);
        listener.onTaskCreated(task);
        listener.onTaskStateChanged(task, previousState, TaskState.QUEUED);

        if (metrics != null) metrics.recordTaskScheduled(task.getType(), task.getGroup());

        log.info("Task submitted: id={}, name={}, priority={}", task.getId(), task.getName(), task.getPriority());
        return task.getId();
    }

    public Optional<Task> getTask(String taskId) {
        return store.findById(taskId);
    }

    public List<Task> getAllTasks() {
        return store.findAll();
    }

    public List<Task> getTasksByState(TaskState state) {
        return store.findByState(state);
    }

    public List<Task> getTasksByGroup(String group) {
        return store.findByGroup(group);
    }

    public boolean cancelTask(String taskId) {
        Optional<Task> taskOpt = store.findById(taskId);
        if (taskOpt.isEmpty()) return false;

        Task task = taskOpt.get();
        if (task.getState().isTerminal()) return false;

        TaskState previousState = task.getState();
        task.setState(TaskState.CANCELLED);
        store.update(task);
        pendingQueue.remove(taskId);
        runningTasks.get(taskId).cancel(true);
        listener.onTaskStateChanged(task, previousState, TaskState.CANCELLED);
        return true;
    }

    public void deleteTask(String taskId) {
        cancelTask(taskId);
        store.delete(taskId);
    }

    public void registerHandler(String taskType, Function<Task, String> handler) {
        taskRunner.registerHandler(taskType, handler);
    }

    public void addListener(TaskListener additionalListener) {
        this.listener.andThen(additionalListener);
    }

    public SchedulerSnapshot getSnapshot() {
        return new SchedulerSnapshot(
                config.getNodeId(),
                clusterManager.isLeader(),
                running.get(),
                store.countAll(),
                store.countByState(TaskState.RUNNING),
                store.countByState(TaskState.QUEUED),
                store.countByState(TaskState.COMPLETED),
                store.countByState(TaskState.FAILED),
                pendingQueue.size(),
                metrics != null ? metrics.getActiveExecutions() : 0
        );
    }

    private void tick() {
        if (!running.get()) return;

        try {
            if (config.isClusterMode() && !clusterManager.isLeader()) {
                reassignOrphanedTasks();
                return;
            }

            processCronScheduledTasks();
            processDependencyReadyTasks();
            processQueuedTasks();
            cleanupCompletedTasks();
        } catch (Exception e) {
            log.error("Scheduler tick failed", e);
        }
    }

    private void processCronScheduledTasks() {
        List<Task> scheduledTasks = store.findScheduledTasks();
        LocalDateTime now = LocalDateTime.now();

        for (Task task : scheduledTasks) {
            if (task.hasCronSchedule()) {
                CronExpression cron = new CronExpression(task.getCronExpression());
                if (task.getNextFireTime() == null || task.getNextFireTime().isBefore(Instant.now())) {
                    TaskState previousState = task.getState();
                    task.setState(TaskState.QUEUED);
                    scheduleNextFire(task);
                    store.update(task);
                    pendingQueue.offer(task);
                    listener.onTaskStateChanged(task, previousState, TaskState.QUEUED);
                }
            }
        }
    }

    private void processDependencyReadyTasks() {
        List<Task> pendingTasks = store.findByState(TaskState.PENDING);
        for (Task task : pendingTasks) {
            if (allDependenciesCompleted(task)) {
                TaskState previousState = task.getState();
                task.setState(TaskState.QUEUED);
                store.update(task);
                pendingQueue.offer(task);
                listener.onTaskStateChanged(task, previousState, TaskState.QUEUED);
            }
        }
    }

    private void processQueuedTasks() {
        while (!pendingQueue.isEmpty() && runningTasks.size() < config.getMaxPoolSize()) {
            Optional<String> taskIdOpt = pendingQueue.poll();
            if (taskIdOpt.isEmpty()) break;

            String taskId = taskIdOpt.get();
            Optional<Task> taskOpt = store.findById(taskId);
            if (taskOpt.isEmpty()) continue;

            Task task = taskOpt.get();
            if (task.getState() != TaskState.QUEUED && task.getState() != TaskState.RETRYING) continue;

            if (config.isClusterMode()) {
                if (!store.claimTask(taskId, config.getNodeId())) continue;
            }

            TaskState previousState = task.getState();
            task.setState(TaskState.RUNNING);
            store.update(task);
            listener.onTaskStateChanged(task, previousState, TaskState.RUNNING);

            CompletableFuture<TaskRunner.TaskExecutionResult> future = taskRunner.execute(task);
            runningTasks.put(taskId, future);

            future.whenComplete((result, error) -> {
                runningTasks.remove(taskId);
                handleExecutionResult(task, result, error);
            });
        }
    }

    private void handleExecutionResult(Task task, TaskRunner.TaskExecutionResult result, Throwable error) {
        try {
            Task currentTask = store.findById(task.getId()).orElse(task);

            if (error != null) {
                handleTaskFailure(currentTask, error);
            } else if (result != null && result.success()) {
                handleTaskSuccess(currentTask);
            } else {
                handleTaskFailure(currentTask, result != null
                        ? new RuntimeException(Objects.requireNonNullElse(result.errorMessage(), "Unknown error"))
                        : new RuntimeException("Execution result is null"));
            }
        } catch (Exception e) {
            log.error("Error handling execution result for task: {}", task.getId(), e);
        }
    }

    private void handleTaskSuccess(Task task) {
        TaskState previousState = task.getState();
        task.setState(TaskState.COMPLETED);
        task.setLastExecutedAt(Instant.now());
        store.update(task);
        listener.onTaskStateChanged(task, previousState, TaskState.COMPLETED);
        log.info("Task completed: id={}", task.getId());

        if (task.hasCronSchedule()) {
            TaskState newPreviousState = task.getState();
            task.setState(TaskState.PENDING);
            store.update(task);
            listener.onTaskStateChanged(task, newPreviousState, TaskState.PENDING);
        }

        unlockDependentTasks(task.getId());
    }

    private void handleTaskFailure(Task task, Throwable error) {
        TaskState previousState = task.getState();
        task.setErrorMessage(error.getMessage());

        if (task.canRetry()) {
            task.incrementRetryCount();
            long delay = task.getRetryPolicy().nextDelayMillis(task.getRetryCount());
            task.setState(TaskState.RETRYING);
            store.update(task);
            listener.onTaskStateChanged(task, previousState, TaskState.RETRYING);
            listener.onTaskRetrying(task, task.getRetryCount(), delay);

            scheduler.schedule(() -> {
                TaskState retryState = TaskState.RETRYING;
                task.setState(TaskState.QUEUED);
                store.update(task);
                pendingQueue.offer(task);
                listener.onTaskStateChanged(task, retryState, TaskState.QUEUED);
            }, delay, TimeUnit.MILLISECONDS);

            if (metrics != null) metrics.recordTaskRetried(task.getType(), task.getGroup());
        } else {
            task.setState(TaskState.FAILED);
            store.update(task);
            listener.onTaskStateChanged(task, previousState, TaskState.FAILED);
            listener.onTaskExecutionFailed(task, error);
            log.error("Task permanently failed: id={}, error={}", task.getId(), error.getMessage());
        }
    }

    private void unlockDependentTasks(String completedTaskId) {
        List<Task> allTasks = store.findAll();
        for (Task task : allTasks) {
            if (task.getState() == TaskState.PENDING && task.getDependencies().contains(completedTaskId)) {
                if (allDependenciesCompleted(task)) {
                    TaskState previousState = task.getState();
                    task.setState(TaskState.QUEUED);
                    store.update(task);
                    pendingQueue.offer(task);
                    listener.onTaskStateChanged(task, previousState, TaskState.QUEUED);
                }
            }
        }
    }

    private void reassignOrphanedTasks() {
        List<String> aliveNodes = heartbeatManager.getAliveNodes();
        List<Task> runningTasks = store.findByState(TaskState.RUNNING);
        for (Task task : runningTasks) {
            if (task.getAssignedNode() != null && !aliveNodes.contains(task.getAssignedNode())) {
                log.warn("Reassigning orphaned task: {} from dead node: {}", task.getId(), task.getAssignedNode());
                store.releaseTask(task.getId());
                TaskState previousState = task.getState();
                task.setState(TaskState.QUEUED);
                store.update(task);
                pendingQueue.offer(task);
                listener.onTaskStateChanged(task, previousState, TaskState.QUEUED);
            }
        }
    }

    private void cleanupCompletedTasks() {
        Instant threshold = Instant.now().minus(Duration.ofHours(24));
        store.deleteExpired(threshold);
    }

    private void cancelRunningTasks() {
        runningTasks.forEach((taskId, future) -> future.cancel(true));
        runningTasks.clear();
    }

    private void validateTask(Task task) {
        Map<String, Task> allTasksMap = new HashMap<>();
        store.findAll().forEach(t -> allTasksMap.put(t.getId(), t));
        allTasksMap.put(task.getId(), task);

        if (!task.getDependencies().isEmpty()) {
            for (String dep : task.getDependencies()) {
                if (!allTasksMap.containsKey(dep) && !dep.equals(task.getId())) {
                    throw new IllegalArgumentException("Dependency not found: " + dep);
                }
            }
            boolean hasCycle = dependencyResolver.wouldCreateCycle(allTasksMap, task.getId(),
                    task.getDependencies().iterator().next());
            if (hasCycle) {
                throw new DependencyResolver.CyclicDependencyException(
                        "Adding task " + task.getId() + " would create a cycle");
            }
        }

        if (task.hasCronSchedule() && !CronExpression.isValid(task.getCronExpression())) {
            throw new IllegalArgumentException("Invalid cron expression: " + task.getCronExpression());
        }
    }

    private void resolveDependencies(Task task) {
        Map<String, Task> allTasksMap = new HashMap<>();
        store.findAll().forEach(t -> allTasksMap.put(t.getId(), t));
        allTasksMap.put(task.getId(), task);
        dependencyResolver.resolve(allTasksMap);
    }

    private boolean allDependenciesCompleted(Task task) {
        for (String depId : task.getDependencies()) {
            Optional<Task> depTask = store.findById(depId);
            if (depTask.isEmpty() || depTask.get().getState() != TaskState.COMPLETED) {
                return false;
            }
        }
        return true;
    }

    private void scheduleNextFire(Task task) {
        CronExpression cron = new CronExpression(task.getCronExpression());
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime next = cron.nextFireTime(now);
        task.setNextFireTime(next.atZone(ZoneId.systemDefault()).toInstant());
    }

    private static TaskStore createDefaultStore(SchedulerConfig config) {
        return new JdbcTaskStore(config);
    }

    @Override
    public void close() {
        shutdown();
    }

    public record SchedulerSnapshot(
            String nodeId,
            boolean isLeader,
            boolean isRunning,
            long totalTasks,
            long runningTasks,
            long queuedTasks,
            long completedTasks,
            long failedTasks,
            int pendingQueueSize,
            long activeExecutions
    ) {}
}
