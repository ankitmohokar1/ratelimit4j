package io.ratelimit4j.redis;

import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a distributed limiter so that a failing backend degrades the limit instead of the service.
 *
 * <p>A limiter that calls Redis on every request has put Redis on the critical path of every request.
 * Deployed bare, that trades a rate-limiting problem for an availability problem: when Redis is slow,
 * every request is slow, and when Redis is down, every request throws.
 *
 * <h2>The circuit breaker</h2>
 *
 * After {@code failureThreshold} consecutive failures the breaker opens and calls stop going to Redis
 * altogether for {@code openDuration}. This is not primarily about the calling thread — it is about
 * not adding load to a struggling Redis, and not paying a connect timeout on every request while it
 * is unreachable. When the window elapses one request is let through to probe; success closes the
 * breaker, failure re-opens it.
 *
 * <p>Only consecutive failures count. A single timeout under load is noise, and a breaker that opens
 * on it would flap between local and distributed enforcement, which is worse than either.
 *
 * <h2>What degradation actually means</h2>
 *
 * Under {@link FailureMode#LOCAL_FALLBACK} the fallback limiter enforces the same policy per node, so
 * across N nodes the fleet allows up to N times the intended rate. That is the honest cost, and it is
 * worth being explicit about: this is not transparent failover, it is a bounded and deliberate
 * loosening of the limit in exchange for staying up.
 *
 * <p>The local limiter is also driven during normal operation, so its state is warm when it is needed
 * — a fallback that starts empty would hand every client a full fresh budget at the exact moment the
 * system is least able to absorb it.
 */
public final class ResilientRateLimiter implements RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(ResilientRateLimiter.class);

    private final RateLimiter distributed;
    private final RateLimiter local;
    private final FailureMode failureMode;
    private final int failureThreshold;
    private final long openDurationNanos;

    private final AtomicLong consecutiveFailures = new AtomicLong();
    private final AtomicLong openedAtNanos = new AtomicLong(Long.MIN_VALUE);
    private final LongAdder degradedDecisions = new LongAdder();
    private final LongAdder backendFailures = new LongAdder();

    public ResilientRateLimiter(RateLimiter distributed, RateLimiter local) {
        this(distributed, local, FailureMode.LOCAL_FALLBACK, 5, Duration.ofSeconds(10));
    }

    public ResilientRateLimiter(
            RateLimiter distributed,
            RateLimiter local,
            FailureMode failureMode,
            int failureThreshold,
            Duration openDuration) {
        this.distributed = Objects.requireNonNull(distributed, "distributed");
        this.local = Objects.requireNonNull(local, "local");
        this.failureMode = Objects.requireNonNull(failureMode, "failureMode");
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be at least 1, got " + failureThreshold);
        }
        Objects.requireNonNull(openDuration, "openDuration");
        if (openDuration.isNegative()) {
            throw new IllegalArgumentException("openDuration must not be negative, got " + openDuration);
        }
        this.failureThreshold = failureThreshold;
        this.openDurationNanos = openDuration.toNanos();
    }

    @Override
    public RateLimitDecision tryAcquire(String key, long permits) {
        return guard(key, permits, true);
    }

    @Override
    public RateLimitDecision peek(String key, long permits) {
        return guard(key, permits, false);
    }

    private RateLimitDecision guard(String key, long permits, boolean consume) {
        if (breakerOpen()) {
            degradedDecisions.increment();
            return degrade(key, permits, consume);
        }
        try {
            RateLimitDecision decision =
                    consume ? distributed.tryAcquire(key, permits) : distributed.peek(key, permits);
            onSuccess(key, permits, consume);
            return decision;
        } catch (RuntimeException e) {
            onFailure(e);
            degradedDecisions.increment();
            return degrade(key, permits, consume);
        }
    }

    /**
     * Keeps the local limiter's state current while the distributed one is healthy.
     *
     * <p>Charged as a peek, never as an acquisition: a warm fallback should reflect roughly what this
     * node has seen, but must not itself refuse traffic that Redis has already allowed.
     */
    private void onSuccess(String key, long permits, boolean consume) {
        consecutiveFailures.set(0);
        if (consume) {
            try {
                local.peek(key, permits);
            } catch (RuntimeException e) {
                // Warming is best-effort; never let it fail a request Redis already decided.
                log.debug("could not warm the local fallback for key {}", key, e);
            }
        }
    }

    private void onFailure(RuntimeException failure) {
        backendFailures.increment();
        long failures = consecutiveFailures.incrementAndGet();
        if (failures == failureThreshold) {
            openedAtNanos.set(System.nanoTime());
            log.warn(
                    "rate limiter backend failed {} times consecutively; opening the breaker for {}ns "
                            + "and degrading to {}",
                    failures,
                    openDurationNanos,
                    failureMode,
                    failure);
        } else {
            log.debug("rate limiter backend failure {} of {}", failures, failureThreshold, failure);
        }
    }

    private boolean breakerOpen() {
        long openedAt = openedAtNanos.get();
        if (openedAt == Long.MIN_VALUE) {
            return false;
        }
        if (System.nanoTime() - openedAt < openDurationNanos) {
            return true;
        }
        // The window has elapsed. Let exactly one caller through to probe; whichever thread wins the
        // CAS pays for the attempt, and the rest keep degrading until it reports back.
        if (openedAtNanos.compareAndSet(openedAt, Long.MIN_VALUE)) {
            consecutiveFailures.set(failureThreshold - 1L);
            log.info("probing the rate limiter backend after {}ns", openDurationNanos);
            return false;
        }
        return true;
    }

    private RateLimitDecision degrade(String key, long permits, boolean consume) {
        return switch (failureMode) {
            case FAIL_OPEN -> RateLimitDecision.allowed(policy().permits(), policy().permits());
            case FAIL_CLOSED -> RateLimitDecision.denied(policy().permits(), 0, Duration.ofSeconds(1));
            case LOCAL_FALLBACK -> consume ? local.tryAcquire(key, permits) : local.peek(key, permits);
        };
    }

    @Override
    public RateLimitPolicy policy() {
        return distributed.policy();
    }

    /**
     * @return whether decisions are currently being made locally rather than by the backend
     */
    public boolean degraded() {
        return openedAtNanos.get() != Long.MIN_VALUE;
    }

    /**
     * @return how many decisions have been made without the backend. Worth alerting on: a non-zero and
     *     rising value means the fleet's effective limit is now N times the configured one.
     */
    public long degradedDecisionCount() {
        return degradedDecisions.sum();
    }

    /**
     * @return how many backend calls have thrown
     */
    public long backendFailureCount() {
        return backendFailures.sum();
    }
}
