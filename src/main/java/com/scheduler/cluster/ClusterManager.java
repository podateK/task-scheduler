package com.scheduler.cluster;

import com.scheduler.SchedulerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class ClusterManager {

    private static final Logger log = LoggerFactory.getLogger(ClusterManager.class);

    private final SchedulerConfig config;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "cluster-manager");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean leader = false;
    private volatile boolean running = false;

    public ClusterManager(SchedulerConfig config) {
        this.config = config;
    }

    public void start() {
        if (!config.isClusterMode()) {
            leader = true;
            log.info("Cluster mode disabled, node is leader by default");
            return;
        }
        running = true;
        scheduler.scheduleWithFixedDelay(this::electLeader,
                0, config.getLeaderElectionInterval().toMillis(), TimeUnit.MILLISECONDS);
        log.info("ClusterManager started for node: {}", config.getNodeId());
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
        if (leader) {
            releaseLeadership();
        }
    }

    public boolean isLeader() {
        return leader;
    }

    private void electLeader() {
        try {
            String lockKey = "scheduler:leader:lock";
            try (var jedis = getJedis()) {
                String currentLeader = jedis.get(lockKey);
                long ttl = jedis.ttl(lockKey);

                if (currentLeader == null || ttl <= 0) {
                    String acquired = jedis.set(lockKey, config.getNodeId(), SetParams.setParams().nx().ex(30));
                    if ("OK".equals(acquired)) {
                        if (!leader) {
                            leader = true;
                            onBecameLeader();
                        }
                        refreshLeadership(jedis, lockKey);
                    } else {
                        String actualLeader = jedis.get(lockKey);
                        if (!config.getNodeId().equals(actualLeader)) {
                            if (leader) {
                                leader = false;
                                onLostLeadership();
                            }
                        }
                    }
                } else if (config.getNodeId().equals(currentLeader)) {
                    refreshLeadership(jedis, lockKey);
                    if (!leader) {
                        leader = true;
                        onBecameLeader();
                    }
                } else {
                    if (leader) {
                        leader = false;
                        onLostLeadership();
                    }
                }
            }
        } catch (Exception e) {
            log.error("Leader election failed for node: {}", config.getNodeId(), e);
        }
    }

    private void refreshLeadership(redis.clients.jedis.Jedis jedis, String lockKey) {
        jedis.expire(lockKey, 30);
    }

    private redis.clients.jedis.Jedis getJedis() {
        return new redis.clients.jedis.Jedis(config.getRedisHost(), config.getRedisPort(),
                (int) config.getRedisConnectionTimeout().toMillis());
    }

    private void releaseLeadership() {
        try (var jedis = getJedis()) {
            String lockKey = "scheduler:leader:lock";
            String currentLeader = jedis.get(lockKey);
            if (config.getNodeId().equals(currentLeader)) {
                jedis.del(lockKey);
            }
            leader = false;
            onLostLeadership();
        } catch (Exception e) {
            log.error("Failed to release leadership", e);
        }
    }

    private void onBecameLeader() {
        log.info("Node {} became LEADER", config.getNodeId());
    }

    private void onLostLeadership() {
        log.info("Node {} lost LEADERSHIP", config.getNodeId());
    }
}
