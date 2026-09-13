package com.scheduler.queue;

import com.scheduler.Task;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PriorityTaskQueue {

    private final TreeMap<Integer,Deque<String>> priorityBuckets = new TreeMap<>(Collections.reverseOrder());
    private final Map<String, Integer> taskPriorities = new HashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public boolean offer(Task task) {
        lock.writeLock().lock();
        try {
            int priority = task.getPriority();
            priorityBuckets.computeIfAbsent(priority, k -> new ArrayDeque<>()).addLast(task.getId());
            taskPriorities.put(task.getId(), priority);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Optional<String> poll() {
        lock.writeLock().lock();
        try {
            Iterator<Map.Entry<Integer, Deque<String>>> it = priorityBuckets.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Integer, Deque<String>> entry = it.next();
                Deque<String> queue = entry.getValue();
                while (!queue.isEmpty()) {
                    String taskId = queue.pollFirst();
                    taskPriorities.remove(taskId);
                    return Optional.of(taskId);
                }
                it.remove();
            }
            return Optional.empty();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean remove(String taskId) {
        lock.writeLock().lock();
        try {
            Integer priority = taskPriorities.remove(taskId);
            if (priority == null) return false;
            Deque<String> queue = priorityBuckets.get(priority);
            if (queue != null) {
                queue.remove(taskId);
                if (queue.isEmpty()) priorityBuckets.remove(priority);
            }
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean contains(String taskId) {
        lock.readLock().lock();
        try {
            return taskPriorities.containsKey(taskId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return taskPriorities.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public List<String> peekAll() {
        lock.readLock().lock();
        try {
            List<String> all = new ArrayList<>();
            for (Deque<String> queue : priorityBuckets.values()) {
                all.addAll(queue);
            }
            return all;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            priorityBuckets.clear();
            taskPriorities.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
