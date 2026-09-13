package com.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.*;

public class JdbcTaskStore implements TaskStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcTaskStore.class);
    private final HikariDataSource dataSource;
    private final ObjectMapper objectMapper;

    public JdbcTaskStore(SchedulerConfig config) {
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getJdbcUrl());
        hikariConfig.setUsername(config.getJdbcUser());
        hikariConfig.setPassword(config.getJdbcPassword());
        hikariConfig.setMaximumPoolSize(config.getHikariMaximumPoolSize());
        hikariConfig.setMinimumIdle(config.getHikariMinimumIdle());
        hikariConfig.setConnectionTimeout(config.getHikariConnectionTimeout().toMillis());
        hikariConfig.setIdleTimeout(config.getHikariIdleTimeout().toMillis());
        hikariConfig.setMaxLifetime(config.getHikariMaxLifetime().toMillis());
        hikariConfig.setPoolName("scheduler-hikari-pool");
        this.dataSource = new HikariDataSource(hikariConfig);
    }

    @Override
    public void initialize() {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id VARCHAR(255) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    type VARCHAR(100) NOT NULL,
                    payload JSONB,
                    dependencies JSONB,
                    priority INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3,
                    retry_policy JSONB,
                    timeout_seconds INTEGER,
                    cron_expression VARCHAR(100),
                    "group" VARCHAR(100) DEFAULT 'default',
                    state VARCHAR(50) DEFAULT 'PENDING',
                    retry_count INTEGER DEFAULT 0,
                    next_fire_time TIMESTAMP WITH TIME ZONE,
                    last_executed_at TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    assigned_node VARCHAR(255),
                    error_message TEXT
                )
            """);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks(\"group\")");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_tasks_next_fire ON tasks(next_fire_time) WHERE next_fire_time IS NOT NULL");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(assigned_node) WHERE assigned_node IS NOT NULL");
            log.info("JdbcTaskStore initialized successfully");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize JdbcTaskStore", e);
        }
    }

    @Override
    public void save(Task task) {
        String sql = """
            INSERT INTO tasks (id, name, type, payload, dependencies, priority, max_retries,
                retry_policy, timeout_seconds, cron_expression, "group", state, retry_count,
                next_fire_time, last_executed_at, created_at, updated_at, assigned_node, error_message)
            VALUES (?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name, type = EXCLUDED.type, payload = EXCLUDED.payload,
                dependencies = EXCLUDED.dependencies, priority = EXCLUDED.priority,
                max_retries = EXCLUDED.max_retries, retry_policy = EXCLUDED.retry_policy,
                timeout_seconds = EXCLUDED.timeout_seconds, cron_expression = EXCLUDED.cron_expression,
                "group" = EXCLUDED."group", state = EXCLUDED.state, retry_count = EXCLUDED.retry_count,
                next_fire_time = EXCLUDED.next_fire_time, last_executed_at = EXCLUDED.last_executed_at,
                updated_at = EXCLUDED.updated_at, assigned_node = EXCLUDED.assigned_node,
                error_message = EXCLUDED.error_message
        """;
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, task.getId());
            ps.setString(2, task.getName());
            ps.setString(3, task.getType());
            ps.setString(4, serialize(task.getPayload()));
            ps.setString(5, serialize(new ArrayList<>(task.getDependencies())));
            ps.setInt(6, task.getPriority());
            ps.setInt(7, task.getMaxRetries());
            ps.setString(8, serialize(Map.of("type", task.getRetryPolicy().getClass().getSimpleName())));
            ps.setObject(9, task.getTimeout() != null ? (int) task.getTimeout().getSeconds() : null);
            ps.setString(10, task.getCronExpression());
            ps.setString(11, task.getGroup());
            ps.setString(12, task.getState().name());
            ps.setInt(13, task.getRetryCount());
            ps.setTimestamp(14, task.getNextFireTime() != null ? Timestamp.from(task.getNextFireTime()) : null);
            ps.setTimestamp(15, task.getLastExecutedAt() != null ? Timestamp.from(task.getLastExecutedAt()) : null);
            ps.setTimestamp(16, Timestamp.from(task.getCreatedAt()));
            ps.setTimestamp(17, Timestamp.from(task.getUpdatedAt()));
            ps.setString(18, task.getAssignedNode());
            ps.setString(19, task.getErrorMessage());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to save task: " + task.getId(), e);
        }
    }

    @Override
    public void update(Task task) {
        save(task);
    }

    @Override
    public Optional<Task> findById(String taskId) {
        String sql = "SELECT * FROM tasks WHERE id = ?";
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return Optional.of(mapRowToTask(rs));
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find task: " + taskId, e);
        }
    }

    @Override
    public List<Task> findAll() {
        return executeQuery("SELECT * FROM tasks ORDER BY priority DESC, created_at ASC");
    }

    @Override
    public List<Task> findByState(TaskState state) {
        return executeQuery("SELECT * FROM tasks WHERE state = ? ORDER BY priority DESC, created_at ASC", state.name());
    }

    @Override
    public List<Task> findByGroup(String group) {
        return executeQuery("SELECT * FROM tasks WHERE \"group\" = ? ORDER BY priority DESC, created_at ASC", group);
    }

    @Override
    public List<Task> findByAssignedNode(String nodeId) {
        return executeQuery("SELECT * FROM tasks WHERE assigned_node = ?", nodeId);
    }

    @Override
    public List<Task> findReadyToExecute() {
        return executeQuery("""
            SELECT * FROM tasks
            WHERE state IN ('QUEUED', 'RETRYING')
            ORDER BY priority DESC, created_at ASC
        """);
    }

    @Override
    public List<Task> findScheduledTasks() {
        return executeQuery("""
            SELECT * FROM tasks
            WHERE cron_expression IS NOT NULL AND state IN ('PENDING', 'COMPLETED')
            AND (next_fire_time IS NULL OR next_fire_time <= NOW())
        """);
    }

    @Override
    public void delete(String taskId) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement("DELETE FROM tasks WHERE id = ?")) {
            ps.setString(1, taskId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete task: " + taskId, e);
        }
    }

    @Override
    public void deleteExpired(Instant before) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM tasks WHERE state = 'COMPLETED' AND updated_at < ?")) {
            ps.setTimestamp(1, Timestamp.from(before));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete expired tasks", e);
        }
    }

    @Override
    public boolean claimTask(String taskId, String nodeId) {
        String sql = """
            UPDATE tasks SET state = 'RUNNING', assigned_node = ?, updated_at = NOW()
            WHERE id = ? AND state IN ('QUEUED', 'RETRYING') AND (assigned_node IS NULL OR assigned_node = '')
        """;
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, nodeId);
            ps.setString(2, taskId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to claim task: " + taskId, e);
        }
    }

    @Override
    public boolean releaseTask(String taskId) {
        String sql = """
            UPDATE tasks SET state = 'QUEUED', assigned_node = NULL, updated_at = NOW()
            WHERE id = ? AND state = 'RUNNING'
        """;
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to release task: " + taskId, e);
        }
    }

    @Override
    public long countByState(TaskState state) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(
                "SELECT COUNT(*) FROM tasks WHERE state = ?")) {
            ps.setString(1, state.name());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count tasks", e);
        }
    }

    @Override
    public long countAll() {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tasks")) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count tasks", e);
        }
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    private List<Task> executeQuery(String sql, Object... params) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            List<Task> tasks = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tasks.add(mapRowToTask(rs));
                }
            }
            return tasks;
        } catch (SQLException e) {
            throw new RuntimeException("Query failed: " + sql, e);
        }
    }

    @SuppressWarnings("unchecked")
    private Task mapRowToTask(ResultSet rs) throws SQLException {
        Map<String, String> payload = deserializeMap(rs.getString("payload"));
        List<String> deps = deserializeList(rs.getString("dependencies"));

        Task.Builder builder = Task.builder()
                .id(rs.getString("id"))
                .name(rs.getString("name"))
                .type(rs.getString("type"))
                .payload(payload)
                .dependencies(new LinkedHashSet<>(deps))
                .priority(rs.getInt("priority"))
                .maxRetries(rs.getInt("max_retries"))
                .group(Objects.requireNonNullElse(rs.getString("group"), "default"))
                .timeout(rs.getObject("timeout_seconds") != null
                        ? java.time.Duration.ofSeconds(rs.getInt("timeout_seconds"))
                        : null)
                .cronExpression(rs.getString("cron_expression"));

        Task task = builder.build();
        for (int i = 0; i < task.getRetryCount() && i < rs.getInt("retry_count"); i++) {
            task.incrementRetryCount();
        }
        Timestamp nextFire = rs.getTimestamp("next_fire_time");
        if (nextFire != null) task.setNextFireTime(nextFire.toInstant());
        Timestamp lastExec = rs.getTimestamp("last_executed_at");
        if (lastExec != null) task.setLastExecutedAt(lastExec.toInstant());
        task.setAssignedNode(rs.getString("assigned_node"));
        task.setErrorMessage(rs.getString("error_message"));

        TaskState state = TaskState.valueOf(rs.getString("state"));
        task.setState(state);
        return task;
    }

    private String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    private Map<String, String> deserializeMap(String json) {
        if (json == null || json.isBlank()) return Map.of();
        try {
            return objectMapper.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            return Map.of();
        }
    }

    private List<String> deserializeList(String json) {
        if (json == null || json.isBlank()) return List.of();
        try {
            return objectMapper.readValue(json, List.class);
        } catch (JsonProcessingException e) {
            return List.of();
        }
    }
}
