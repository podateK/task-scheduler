package com.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class RedisTaskStore implements TaskStore {

    private static final Logger log = LoggerFactory.getLogger(RedisTaskStore.class);
    private static final String TASK_PREFIX = "scheduler:task:";
    private static final String INDEX_PREFIX = "scheduler:tasks:";
    private static final String STATE_PREFIX = "scheduler:state:";
    private static final String GROUP_PREFIX = "scheduler:group:";
    private static final String NODE_PREFIX = "scheduler:node:";
    private static final String METADATA_KEY = "scheduler:metadata";

    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper;

    public RedisTaskStore(SchedulerConfig config) {
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(config.getRedisMaxTotal());
        poolConfig.setMaxIdle(config.getRedisMaxTotal() / 2);
        poolConfig.setMinIdle(2);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);
        this.jedisPool = new JedisPool(poolConfig, config.getRedisHost(), config.getRedisPort(),
                (int) config.getRedisConnectionTimeout().toMillis(),
                config.getRedisPassword().isEmpty() ? null : config.getRedisPassword(),
                config.getRedisDatabase());
    }

    @Override
    public void initialize() {
        try (var jedis = jedisPool.getResource()) {
            jedis.ping();
            log.info("RedisTaskStore initialized successfully");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize RedisTaskStore", e);
        }
    }

    @Override
    public void save(Task task) {
        try (var jedis = jedisPool.getResource()) {
            String json = serialize(task);
            String key = TASK_PREFIX + task.getId();
            jedis.set(key, json);

            jedis.sadd(INDEX_PREFIX + "all", task.getId());
            jedis.sadd(STATE_PREFIX + task.getState().name(), task.getId());
            jedis.sadd(GROUP_PREFIX + task.getGroup(), task.getId());
            if (task.getAssignedNode() != null) {
                jedis.sadd(NODE_PREFIX + task.getAssignedNode(), task.getId());
            }
            if (task.getNextFireTime() != null) {
                jedis.zadd(INDEX_PREFIX + "scheduled", task.getNextFireTime().toEpochMilli(), task.getId());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to save task: " + task.getId(), e);
        }
    }

    @Override
    public void update(Task task) {
        try (var jedis = jedisPool.getResource()) {
            Task existing = findById(task.getId()).orElse(null);
            if (existing != null) {
                cleanIndex(jedis, existing);
            }
            save(task);
        }
    }

    @Override
    public Optional<Task> findById(String taskId) {
        try (var jedis = jedisPool.getResource()) {
            String json = jedis.get(TASK_PREFIX + taskId);
            if (json == null) return Optional.empty();
            return Optional.of(deserialize(json));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find task: " + taskId, e);
        }
    }

    @Override
    public List<Task> findAll() {
        return findByIds(getAllIds("all"));
    }

    @Override
    public List<Task> findByState(TaskState state) {
        return findByIds(getAllIds("state:" + state.name()));
    }

    @Override
    public List<Task> findByGroup(String group) {
        return findByIds(getAllIds("group:" + group));
    }

    @Override
    public List<Task> findByAssignedNode(String nodeId) {
        return findByIds(getAllIds("node:" + nodeId));
    }

    @Override
    public List<Task> findReadyToExecute() {
        List<Task> queued = findByState(TaskState.QUEUED);
        List<Task> retrying = findByState(TaskState.RETRYING);
        return Stream.concat(queued.stream(), retrying.stream())
                .sorted(Comparator.comparingInt(Task::getPriority).reversed())
                .toList();
    }

    @Override
    public List<Task> findScheduledTasks() {
        try (var jedis = jedisPool.getResource()) {
            long now = Instant.now().toEpochMilli();
            Set<String> ids = jedis.zrangeByScore(INDEX_PREFIX + "scheduled", 0, now);
            return findByIds(new ArrayList<>(ids));
        } catch (Exception e) {
            throw new RuntimeException("Failed to find scheduled tasks", e);
        }
    }

    @Override
    public void delete(String taskId) {
        try (var jedis = jedisPool.getResource()) {
            Task task = findById(taskId).orElse(null);
            if (task != null) {
                cleanIndex(jedis, task);
                jedis.del(TASK_PREFIX + taskId);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete task: " + taskId, e);
        }
    }

    @Override
    public void deleteExpired(Instant before) {
        try (var jedis = jedisPool.getResource()) {
            Set<String> completedIds = getAllIds("state:COMPLETED");
            for (String id : completedIds) {
                Task task = findById(id).orElse(null);
                if (task != null && task.getUpdatedAt().isBefore(before)) {
                    delete(id);
                }
            }
        }
    }

    @Override
    public boolean claimTask(String taskId, String nodeId) {
        try (var jedis = jedisPool.getResource()) {
            String key = TASK_PREFIX + taskId;
            String json = jedis.get(key);
            if (json == null) return false;

            Task task = deserialize(json);
            if (task.getState() != TaskState.QUEUED && task.getState() != TaskState.RETRYING) return false;

            cleanIndex(jedis, task);
            task.setState(TaskState.RUNNING);
            task.setAssignedNode(nodeId);
            save(task);
            return true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to claim task: " + taskId, e);
        }
    }

    @Override
    public boolean releaseTask(String taskId) {
        try (var jedis = jedisPool.getResource()) {
            String key = TASK_PREFIX + taskId;
            String json = jedis.get(key);
            if (json == null) return false;

            Task task = deserialize(json);
            if (task.getState() != TaskState.RUNNING) return false;

            cleanIndex(jedis, task);
            task.setState(TaskState.QUEUED);
            task.setAssignedNode(null);
            save(task);
            return true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to release task: " + taskId, e);
        }
    }

    @Override
    public long countByState(TaskState state) {
        try (var jedis = jedisPool.getResource()) {
            return jedis.scard(STATE_PREFIX + state.name());
        }
    }

    @Override
    public long countAll() {
        try (var jedis = jedisPool.getResource()) {
            return jedis.scard(INDEX_PREFIX + "all");
        }
    }

    @Override
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }

    private Set<String> getAllIds(String setKey) {
        try (var jedis = jedisPool.getResource()) {
            return jedis.smembers(INDEX_PREFIX + setKey);
        }
    }

    private List<Task> findByIds(Collection<String> ids) {
        if (ids.isEmpty()) return List.of();
        try (var jedis = jedisPool.getResource()) {
            String[] keys = ids.stream().map(id -> TASK_PREFIX + id).toArray(String[]::new);
            List<String> jsons = jedis.mget(keys);
            return jsons.stream()
                    .filter(Objects::nonNull)
                    .map(this::deserialize)
                    .toList();
        }
    }

    private void cleanIndex(redis.clients.jedis.Jedis jedis, Task task) {
        jedis.srem(INDEX_PREFIX + "all", task.getId());
        jedis.srem(STATE_PREFIX + task.getState().name(), task.getId());
        jedis.srem(GROUP_PREFIX + task.getGroup(), task.getId());
        if (task.getAssignedNode() != null) {
            jedis.srem(NODE_PREFIX + task.getAssignedNode(), task.getId());
        }
        jedis.zrem(INDEX_PREFIX + "scheduled", task.getId());
    }

    private String serialize(Task task) {
        try {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("id", task.getId());
            data.put("name", task.getName());
            data.put("type", task.getType());
            data.put("payload", task.getPayload());
            data.put("dependencies", new ArrayList<>(task.getDependencies()));
            data.put("priority", task.getPriority());
            data.put("maxRetries", task.getMaxRetries());
            data.put("timeout", task.getTimeout() != null ? task.getTimeout().getSeconds() : null);
            data.put("cronExpression", task.getCronExpression());
            data.put("group", task.getGroup());
            data.put("state", task.getState().name());
            data.put("retryCount", task.getRetryCount());
            data.put("nextFireTime", task.getNextFireTime() != null ? task.getNextFireTime().toEpochMilli() : null);
            data.put("lastExecutedAt", task.getLastExecutedAt() != null ? task.getLastExecutedAt().toEpochMilli() : null);
            data.put("createdAt", task.getCreatedAt().toEpochMilli());
            data.put("updatedAt", task.getUpdatedAt().toEpochMilli());
            data.put("assignedNode", task.getAssignedNode());
            data.put("errorMessage", task.getErrorMessage());
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    @SuppressWarnings("unchecked")
    private Task deserialize(String json) {
        try {
            Map<String, Object> data = objectMapper.readValue(json, Map.class);
            Task.Builder builder = Task.builder()
                    .id((String) data.get("id"))
                    .name((String) data.get("name"))
                    .type((String) data.get("type"))
                    .payload(data.get("payload") instanceof Map m ? (Map<String, String>) m : Map.of())
                    .dependencies(data.get("dependencies") instanceof List l
                            ? new LinkedHashSet<>((List<String>) l)
                            : new LinkedHashSet<>())
                    .priority(data.get("priority") instanceof Integer i ? i : 0)
                    .maxRetries(data.get("maxRetries") instanceof Integer i ? i : 3)
                    .group(data.get("group") instanceof String s ? s : "default");

            if (data.get("timeout") instanceof Number n) {
                builder.timeout(java.time.Duration.ofSeconds(n.longValue()));
            }
            if (data.get("cronExpression") instanceof String s && !s.isBlank()) {
                builder.cronExpression(s);
            }

            Task task = builder.build();
            for (int i = 0; i < (data.get("retryCount") instanceof Integer rc ? rc : 0); i++) {
                task.incrementRetryCount();
            }
            if (data.get("nextFireTime") instanceof Number n) {
                task.setNextFireTime(Instant.ofEpochMilli(n.longValue()));
            }
            if (data.get("lastExecutedAt") instanceof Number n) {
                task.setLastExecutedAt(Instant.ofEpochMilli(n.longValue()));
            }
            if (data.get("assignedNode") instanceof String s) task.setAssignedNode(s);
            if (data.get("errorMessage") instanceof String s) task.setErrorMessage(s);

            TaskState state = TaskState.valueOf((String) data.get("state"));
            task.setState(state);
            return task;
        } catch (Exception e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}
