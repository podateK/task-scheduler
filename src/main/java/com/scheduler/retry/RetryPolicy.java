package com.scheduler.retry;

import java.time.Duration;

public sealed interface RetryPolicy permits RetryPolicy.FixedDelay, RetryPolicy.ExponentialBackoff, RetryPolicy.Custom {

    long nextDelayMillis(int attemptNumber);

    static RetryPolicy fixedDelay(Duration delay) {
        return new FixedDelay(delay);
    }

    static RetryPolicy exponentialBackoff(Duration initialDelay, Duration maxDelay, double multiplier) {
        return new ExponentialBackoff(initialDelay, maxDelay, multiplier);
    }

    static RetryPolicy custom(java.util.function.Function<Integer, Duration> delayFunction) {
        return new Custom(delayFunction);
    }

    record FixedDelay(Duration delay) implements RetryPolicy {
        @Override
        public long nextDelayMillis(int attemptNumber) {
            return delay.toMillis();
        }
    }

    record ExponentialBackoff(Duration initialDelay, Duration maxDelay, double multiplier) implements RetryPolicy {
        @Override
        public long nextDelayMillis(int attemptNumber) {
            long delay = (long) (initialDelay.toMillis() * Math.pow(multiplier, attemptNumber - 1));
            return Math.min(delay, maxDelay.toMillis());
        }
    }

    record Custom(java.util.function.Function<Integer, Duration> delayFunction) implements RetryPolicy {
        @Override
        public long nextDelayMillis(int attemptNumber) {
            return delayFunction.apply(attemptNumber).toMillis();
        }
    }
}
