package com.scheduler;

import java.time.Duration;
import java.util.Properties;

public class SchedulerConfig {

    private int corePoolSize = Runtime.getRuntime().availableProcessors();
    private int maxPoolSize = corePoolSize * 2;
    private Duration taskTimeout = Duration.ofMinutes(5);
    private Duration shutdownTimeout = Duration.ofSeconds(30);
    private Duration scheduleInterval = Duration.ofSeconds(1);
    private Duration heartbeatInterval = Duration.ofSeconds(5);
    private Duration leaderElectionInterval = Duration.ofSeconds(10);
    private String nodeId = generateNodeId();
    private boolean clusterMode = false;
    private boolean metricsEnabled = true;
    private int apiPort = 8080;

    private String jdbcUrl = "jdbc:postgresql://localhost:5432/task_scheduler";
    private String jdbcUser = "scheduler";
    private String jdbcPassword = "";
    private int hikariMaximumPoolSize = 10;
    private int hikariMinimumIdle = 2;
    private Duration hikariConnectionTimeout = Duration.ofSeconds(30);
    private Duration hikariIdleTimeout = Duration.ofMinutes(10);
    private Duration hikariMaxLifetime = Duration.ofMinutes(30);

    private String redisHost = "localhost";
    private int redisPort = 6379;
    private String redisPassword = "";
    private int redisDatabase = 0;
    private Duration redisConnectionTimeout = Duration.ofSeconds(5);
    private int redisMaxTotal = 20;

    public int getCorePoolSize() { return corePoolSize; }
    public void setCorePoolSize(int corePoolSize) { this.corePoolSize = corePoolSize; }
    public int getMaxPoolSize() { return maxPoolSize; }
    public void setMaxPoolSize(int maxPoolSize) { this.maxPoolSize = maxPoolSize; }
    public Duration getTaskTimeout() { return taskTimeout; }
    public void setTaskTimeout(Duration taskTimeout) { this.taskTimeout = taskTimeout; }
    public Duration getShutdownTimeout() { return shutdownTimeout; }
    public void setShutdownTimeout(Duration shutdownTimeout) { this.shutdownTimeout = shutdownTimeout; }
    public Duration getScheduleInterval() { return scheduleInterval; }
    public void setScheduleInterval(Duration scheduleInterval) { this.scheduleInterval = scheduleInterval; }
    public Duration getHeartbeatInterval() { return heartbeatInterval; }
    public void setHeartbeatInterval(Duration heartbeatInterval) { this.heartbeatInterval = heartbeatInterval; }
    public Duration getLeaderElectionInterval() { return leaderElectionInterval; }
    public void setLeaderElectionInterval(Duration leaderElectionInterval) { this.leaderElectionInterval = leaderElectionInterval; }
    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public boolean isClusterMode() { return clusterMode; }
    public void setClusterMode(boolean clusterMode) { this.clusterMode = clusterMode; }
    public boolean isMetricsEnabled() { return metricsEnabled; }
    public void setMetricsEnabled(boolean metricsEnabled) { this.metricsEnabled = metricsEnabled; }
    public int getApiPort() { return apiPort; }
    public void setApiPort(int apiPort) { this.apiPort = apiPort; }
    public String getJdbcUrl() { return jdbcUrl; }
    public void setJdbcUrl(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }
    public String getJdbcUser() { return jdbcUser; }
    public void setJdbcUser(String jdbcUser) { this.jdbcUser = jdbcUser; }
    public String getJdbcPassword() { return jdbcPassword; }
    public void setJdbcPassword(String jdbcPassword) { this.jdbcPassword = jdbcPassword; }
    public int getHikariMaximumPoolSize() { return hikariMaximumPoolSize; }
    public void setHikariMaximumPoolSize(int hikariMaximumPoolSize) { this.hikariMaximumPoolSize = hikariMaximumPoolSize; }
    public int getHikariMinimumIdle() { return hikariMinimumIdle; }
    public void setHikariMinimumIdle(int hikariMinimumIdle) { this.hikariMinimumIdle = hikariMinimumIdle; }
    public Duration getHikariConnectionTimeout() { return hikariConnectionTimeout; }
    public void setHikariConnectionTimeout(Duration hikariConnectionTimeout) { this.hikariConnectionTimeout = hikariConnectionTimeout; }
    public Duration getHikariIdleTimeout() { return hikariIdleTimeout; }
    public void setHikariIdleTimeout(Duration hikariIdleTimeout) { this.hikariIdleTimeout = hikariIdleTimeout; }
    public Duration getHikariMaxLifetime() { return hikariMaxLifetime; }
    public void setHikariMaxLifetime(Duration hikariMaxLifetime) { this.hikariMaxLifetime = hikariMaxLifetime; }
    public String getRedisHost() { return redisHost; }
    public void setRedisHost(String redisHost) { this.redisHost = redisHost; }
    public int getRedisPort() { return redisPort; }
    public void setRedisPort(int redisPort) { this.redisPort = redisPort; }
    public String getRedisPassword() { return redisPassword; }
    public void setRedisPassword(String redisPassword) { this.redisPassword = redisPassword; }
    public int getRedisDatabase() { return redisDatabase; }
    public void setRedisDatabase(int redisDatabase) { this.redisDatabase = redisDatabase; }
    public Duration getRedisConnectionTimeout() { return redisConnectionTimeout; }
    public void setRedisConnectionTimeout(Duration redisConnectionTimeout) { this.redisConnectionTimeout = redisConnectionTimeout; }
    public int getRedisMaxTotal() { return redisMaxTotal; }
    public void setRedisMaxTotal(int redisMaxTotal) { this.redisMaxTotal = redisMaxTotal; }

    public static SchedulerConfig fromProperties(Properties properties) {
        SchedulerConfig config = new SchedulerConfig();
        applyIfPresent(properties, "scheduler.core-pool-size", Integer.class, config::setCorePoolSize);
        applyIfPresent(properties, "scheduler.max-pool-size", Integer.class, config::setMaxPoolSize);
        applyIfPresent(properties, "scheduler.node-id", String.class, config::setNodeId);
        applyIfPresent(properties, "scheduler.cluster-mode", Boolean.class, config::setClusterMode);
        applyIfPresent(properties, "scheduler.metrics-enabled", Boolean.class, config::setMetricsEnabled);
        applyIfPresent(properties, "scheduler.api-port", Integer.class, config::setApiPort);
        applyIfPresent(properties, "scheduler.jdbc.url", String.class, config::setJdbcUrl);
        applyIfPresent(properties, "scheduler.jdbc.user", String.class, config::setJdbcUser);
        applyIfPresent(properties, "scheduler.jdbc.password", String.class, config::setJdbcPassword);
        applyIfPresent(properties, "scheduler.redis.host", String.class, config::setRedisHost);
        applyIfPresent(properties, "scheduler.redis.port", Integer.class, config::setRedisPort);
        applyIfPresent(properties, "scheduler.redis.password", String.class, config::setRedisPassword);
        return config;
    }

    private static <T> void applyIfPresent(Properties props, String key, Class<T> type, java.util.function.Consumer<T> setter) {
        String value = props.getProperty(key);
        if (value != null) {
            T converted = switch (type.getSimpleName()) {
                case "Integer" -> type.cast(Integer.parseInt(value));
                case "Boolean" -> type.cast(Boolean.parseBoolean(value));
                case "Long" -> type.cast(Long.parseLong(value));
                default -> type.cast(value);
            };
            setter.accept(converted);
        }
    }

    private static String generateNodeId() {
        return "node-%s-%d".formatted(
                System.getenv().getOrDefault("HOSTNAME", System.getenv().getOrDefault("COMPUTERNAME", "unknown")),
                ProcessHandle.current().pid()
        );
    }
}
