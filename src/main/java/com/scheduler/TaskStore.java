package com.scheduler;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface TaskStore {

    void initialize();

    void save(Task task);

    void update(Task task);

    Optional<Task> findById(String taskId);

    List<Task> findAll();

    List<Task> findByState(TaskState state);

    List<Task> findByGroup(String group);

    List<Task> findByAssignedNode(String nodeId);

    List<Task> findReadyToExecute();

    List<Task> findScheduledTasks();

    void delete(String taskId);

    void deleteExpired(Instant before);

    boolean claimTask(String taskId, String nodeId);

    boolean releaseTask(String taskId);

    long countByState(TaskState state);

    long countAll();

    void close();
}
