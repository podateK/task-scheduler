package com.scheduler.cluster;

import com.scheduler.SchedulerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class HeartbeatManager {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatManager.class);
    private static final String HEARTBEAT_KEY_PREFIX = "scheduler:heartbeat:";
    private static final String NODES_KEY = "scheduler:nodes";

    private final SchedulerConfig config;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "heartbeat-manager");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean running = false;

    public HeartbeatManager(SchedulerConfig config) {
        this.config = config;
    }

    public void start() {
        if (!config.isClusterMode()) {
            log.info("Cluster mode disabled, heartbeat not started");
            return;
        }
        running = true;
        registerNode();
        scheduler.scheduleWithFixedDelay(this::sendHeartbeat,
                0, config.getHeartbeatInterval().toMillis(), TimeUnit.MILLISECONDS);
        log.info("HeartbeatManager started for node: {}", config.getNodeId());
    }

    public void stop() {
        running = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        deregisterNode();
    }

    public List<String> getAliveNodes() {
        try (Jedis jedis = createJedis()) {
            Set<String> nodeIds = jedis.smembers(NODES_KEY);
            List<String> aliveNodes = new ArrayList<>();
            long now = Instant.now().toEpochMilli();
            for (String nodeId : nodeIds) {
                String heartbeat = jedis.get(HEARTBEAT_KEY_PREFIX + nodeId);
                if (heartbeat != null) {
                    long timestamp = Long.parseLong(heartbeat);
                    if (now - timestamp < config.getHeartbeatInterval().toMillis() * 3) {
                        aliveNodes.add(nodeId);
                    }
                }
            }
            return aliveNodes;
        } catch (Exception e) {
            log.error("Failed to get alive nodes", e);
            return List.of(config.getNodeId());
        }
    }

    public boolean isNodeAlive(String nodeId) {
        return getAliveNodes().contains(nodeId);
    }

    public Map<String, Instant> getNodeHeartbeats() {
        try (Jedis jedis = createJedis()) {
            Set<String> nodeIds = jedis.smembers(NODES_KEY);
            Map<String, Instant> heartbeats = new HashMap<>();
            for (String nodeId : nodeIds) {
                String value = jedis.get(HEARTBEAT_KEY_PREFIX + nodeId);
                if (value != null) {
                    heartbeats.put(nodeId, Instant.ofEpochMilli(Long.parseLong(value)));
                }
            }
            return heartbeats;
        }
    }

    private void registerNode() {
        try (Jedis jedis = createJedis()) {
            jedis.sadd(NODES_KEY, config.getNodeId());
            sendHeartbeat();
        } catch (Exception e) {
            log.error("Failed to register node: {}", config.getNodeId(), e);
        }
    }

    private void deregisterNode() {
        try (Jedis jedis = createJedis()) {
            jedis.srem(NODES_KEY, config.getNodeId());
            jedis.del(HEARTBEAT_KEY_PREFIX + config.getNodeId());
            log.info("Node {} deregistered", config.getNodeId());
        } catch (Exception e) {
            log.error("Failed to deregister node: {}", config.getNodeId(), e);
        }
    }

    private void sendHeartbeat() {
        try (Jedis jedis = createJedis()) {
            String key = HEARTBEAT_KEY_PREFIX + config.getNodeId();
            String now = String.valueOf(Instant.now().toEpochMilli());
            jedis.set(key, now, SetParams.setParams().ex(
                    (int) (config.getHeartbeatInterval().toMillis() * 3 / 1000)));
            jedis.sadd(NODES_KEY, config.getNodeId());
            cleanStaleNodes(jedis);
        } catch (Exception e) {
            log.error("Failed to send heartbeat for node: {}", config.getNodeId(), e);
        }
    }

    private void cleanStaleNodes(Jedis jedis) {
        long now = Instant.now().toEpochMilli();
        long maxAge = config.getHeartbeatInterval().toMillis() * 5;
        Set<String> nodeIds = jedis.smembers(NODES_KEY);
        for (String nodeId : nodeIds) {
            if (nodeId.equals(config.getNodeId())) continue;
            String heartbeat = jedis.get(HEARTBEAT_KEY_PREFIX + nodeId);
            if (heartbeat == null || now - Long.parseLong(heartbeat) > maxAge) {
                jedis.srem(NODES_KEY, nodeId);
                jedis.del(HEARTBEAT_KEY_PREFIX + nodeId);
                log.info("Removed stale node: {}", nodeId);
            }
        }
    }

    private Jedis createJedis() {
        return new Jedis(config.getRedisHost(), config.getRedisPort(),
                (int) config.getRedisConnectionTimeout().toMillis());
    }
}
