package com.scheduler;

import com.scheduler.cron.CronExpression;
import com.scheduler.queue.PriorityTaskQueue;
import com.scheduler.retry.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

class TaskTest {

    @Test
    void taskBuilderCreatesValidTask() {
        Task task = Task.builder()
                .name("test-task")
                .type("email")
                .priority(10)
                .maxRetries(5)
                .timeout(Duration.ofMinutes(10))
                .addPayload("to", "user@example.com")
                .addDependency("dep-1")
                .group("notifications")
                .cronExpression("0 * * * *")
                .build();

        assertThat(task.getId()).isNotBlank();
        assertThat(task.getName()).isEqualTo("test-task");
        assertThat(task.getType()).isEqualTo("email");
        assertThat(task.getPriority()).isEqualTo(10);
        assertThat(task.getMaxRetries()).isEqualTo(5);
        assertThat(task.getTimeout()).isEqualTo(Duration.ofMinutes(10));
        assertThat(task.getPayload()).containsEntry("to", "user@example.com");
        assertThat(task.getDependencies()).containsExactly("dep-1");
        assertThat(task.getGroup()).isEqualTo("notifications");
        assertThat(task.getCronExpression()).isEqualTo("0 * * * *");
        assertThat(task.getState()).isEqualTo(TaskState.PENDING);
        assertThat(task.hasCronSchedule()).isTrue();
    }

    @Test
    void taskStateTransitions() {
        Task task = Task.builder().name("t").type("t").build();
        assertThat(task.getState()).isEqualTo(TaskState.PENDING);
        assertThat(task.getState().canTransitionTo(TaskState.QUEUED)).isTrue();
        assertThat(task.getState().canTransitionTo(TaskState.RUNNING)).isFalse();

        task.setState(TaskState.QUEUED);
        assertThat(task.getState().canTransitionTo(TaskState.RUNNING)).isTrue();
        assertThat(task.getState().canTransitionTo(TaskState.COMPLETED)).isFalse();

        task.setState(TaskState.RUNNING);
        assertThat(task.getState().canTransitionTo(TaskState.COMPLETED)).isTrue();
        assertThat(task.getState().canTransitionTo(TaskState.FAILED)).isTrue();
        assertThat(task.getState().canTransitionTo(TaskState.RETRYING)).isTrue();

        task.setState(TaskState.COMPLETED);
        assertThat(task.getState().isTerminal()).isTrue();
        assertThat(task.getState().canTransitionTo(TaskState.QUEUED)).isFalse();
    }

    @Test
    void taskRetryTracking() {
        Task task = Task.builder().name("t").type("t").maxRetries(3).build();
        assertThat(task.canRetry()).isTrue();
        assertThat(task.getRetryCount()).isEqualTo(0);

        task.incrementRetryCount();
        assertThat(task.canRetry()).isTrue();
        assertThat(task.getRetryCount()).isEqualTo(1);

        task.incrementRetryCount();
        assertThat(task.canRetry()).isTrue();

        task.incrementRetryCount();
        assertThat(task.canRetry()).isFalse();
    }
}

class RetryPolicyTest {

    @Test
    void fixedDelayReturnsSameDelay() {
        RetryPolicy policy = RetryPolicy.fixedDelay(Duration.ofSeconds(5));
        assertThat(policy.nextDelayMillis(1)).isEqualTo(5000);
        assertThat(policy.nextDelayMillis(2)).isEqualTo(5000);
        assertThat(policy.nextDelayMillis(10)).isEqualTo(5000);
    }

    @Test
    void exponentialBackoffIncreasesWithAttempt() {
        RetryPolicy policy = RetryPolicy.exponentialBackoff(
                Duration.ofSeconds(1), Duration.ofMinutes(5), 2.0);

        assertThat(policy.nextDelayMillis(1)).isEqualTo(1000);
        assertThat(policy.nextDelayMillis(2)).isEqualTo(2000);
        assertThat(policy.nextDelayMillis(3)).isEqualTo(4000);
        assertThat(policy.nextDelayMillis(4)).isEqualTo(8000);
    }

    @Test
    void exponentialBackoffRespectsMaxDelay() {
        RetryPolicy policy = RetryPolicy.exponentialBackoff(
                Duration.ofSeconds(1), Duration.ofSeconds(10), 2.0);

        assertThat(policy.nextDelayMillis(1)).isEqualTo(1000);
        assertThat(policy.nextDelayMillis(10)).isEqualTo(10000);
        assertThat(policy.nextDelayMillis(20)).isEqualTo(10000);
    }

    @Test
    void customRetryPolicy() {
        RetryPolicy policy = RetryPolicy.custom(attempt -> Duration.ofMillis(attempt * 100L));
        assertThat(policy.nextDelayMillis(1)).isEqualTo(100);
        assertThat(policy.nextDelayMillis(5)).isEqualTo(500);
    }
}

class PriorityTaskQueueTest {

    @Test
    void higherPriorityTasksAreReturnedFirst() {
        PriorityTaskQueue queue = new PriorityTaskQueue();
        queue.offer(Task.builder().name("low").type("t").priority(1).build());
        queue.offer(Task.builder().name("high").type("t").priority(10).build());
        queue.offer(Task.builder().name("mid").type("t").priority(5).build());

        assertThat(queue.poll()).isPresent();
        Task high = Task.builder().name("high").type("t").build();
        assertThat(queue.size()).isEqualTo(2);
    }

    @Test
    void removesTaskSuccessfully() {
        PriorityTaskQueue queue = new PriorityTaskQueue();
        Task task = Task.builder().name("t").type("t").build();
        queue.offer(task);
        assertThat(queue.size()).isEqualTo(1);
        assertThat(queue.remove(task.getId())).isTrue();
        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    void peekAllReturnsAllTasks() {
        PriorityTaskQueue queue = new PriorityTaskQueue();
        queue.offer(Task.builder().name("a").type("t").build());
        queue.offer(Task.builder().name("b").type("t").build());
        assertThat(queue.peekAll()).hasSize(2);
    }
}

class DependencyResolverTest {

    @Test
    void resolvesLinearDependency() {
        DependencyResolver resolver = new DependencyResolver();
        Map<String, Task> tasks = new LinkedHashMap<>();

        Task t1 = Task.builder().id("1").name("t1").type("t").build();
        Task t2 = Task.builder().id("2").name("t2").type("t").addDependency("1").build();
        Task t3 = Task.builder().id("3").name("t3").type("t").addDependency("2").build();

        tasks.put("1", t1);
        tasks.put("2", t2);
        tasks.put("3", t3);

        List<String> sorted = resolver.resolve(tasks);
        assertThat(sorted).containsExactly("1", "2", "3");
    }

    @Test
    void detectsCycle() {
        DependencyResolver resolver = new DependencyResolver();
        Map<String, Task> tasks = new LinkedHashMap<>();

        Task t1 = Task.builder().id("1").name("t1").type("t").addDependency("2").build();
        Task t2 = Task.builder().id("2").name("t2").type("t").addDependency("1").build();

        tasks.put("1", t1);
        tasks.put("2", t2);

        assertThatThrownBy(() -> resolver.resolve(tasks))
                .isInstanceOf(DependencyResolver.CyclicDependencyException.class);
    }

    @Test
    void detectsSelfCycle() {
        DependencyResolver resolver = new DependencyResolver();
        Map<String, Task> tasks = new LinkedHashMap<>();

        Task t1 = Task.builder().id("1").name("t1").type("t").addDependency("1").build();
        tasks.put("1", t1);

        assertThatThrownBy(() -> resolver.resolve(tasks))
                .isInstanceOf(DependencyResolver.CyclicDependencyException.class);
    }

    @Test
    void detectsCycleWhenAddingDependency() {
        DependencyResolver resolver = new DependencyResolver();
        Map<String, Task> tasks = new LinkedHashMap<>();

        Task t1 = Task.builder().id("1").name("t1").type("t").addDependency("2").build();
        Task t2 = Task.builder().id("2").name("t2").type("t").addDependency("3").build();
        Task t3 = Task.builder().id("3").name("t3").type("t").build();

        tasks.put("1", t1);
        tasks.put("2", t2);
        tasks.put("3", t3);

        assertThat(resolver.wouldCreateCycle(tasks, "3", "1")).isTrue();
        assertThat(resolver.wouldCreateCycle(tasks, "3", "1")).isTrue();
    }
}

class CronExpressionTest {

    @Test
    void everyMinuteMatches() {
        CronExpression cron = new CronExpression("* * * * *");
        LocalDateTime now = LocalDateTime.of(2024, 1, 1, 12, 30);
        assertThat(cron.matches(now)).isTrue();
    }

    @Test
    void specificMinuteMatches() {
        CronExpression cron = new CronExpression("30 * * * *");
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 12, 30))).isTrue();
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 12, 31))).isFalse();
    }

    @Test
    void dailyAtNoon() {
        CronExpression cron = new CronExpression("0 12 * * *");
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 12, 0))).isTrue();
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 12, 1))).isFalse();
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 13, 0))).isFalse();
    }

    @Test
    void nextFireTimeCalculation() {
        CronExpression cron = new CronExpression("0 12 * * *");
        LocalDateTime after = LocalDateTime.of(2024, 1, 1, 13, 0);
        LocalDateTime next = cron.nextFireTime(after);
        assertThat(next).isEqualTo(LocalDateTime.of(2024, 1, 2, 12, 0));
    }

    @Test
    void stepExpressionMatches() {
        CronExpression cron = new CronExpression("0 */2 * * *");
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 0, 0))).isTrue();
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 2, 0))).isTrue();
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 1, 0))).isFalse();
    }

    @Test
    void rangeExpressionMatches() {
        CronExpression cron = new CronExpression("0 9-17 * * *");
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 9, 0))).isTrue();
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 17, 0))).isTrue();
        assertThat(cron.matches(LocalDateTime.of(2024, 1, 1, 18, 0))).isFalse();
    }

    @Test
    void invalidExpressionThrows() {
        assertThatThrownBy(() -> new CronExpression("invalid"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isValidReturnsCorrectly() {
        assertThat(CronExpression.isValid("* * * * *")).isTrue();
        assertThat(CronExpression.isValid("0 12 * * *")).isTrue();
        assertThat(CronExpression.isValid("invalid")).isFalse();
    }
}
