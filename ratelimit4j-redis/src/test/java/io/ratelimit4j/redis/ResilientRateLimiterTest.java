package io.ratelimit4j.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.algorithm.Algorithm;
import io.ratelimit4j.core.time.AdjustableTicker;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Failure-path tests for the resilience wrapper.
 *
 * <p>These deliberately need no Redis. The behaviour under test is what happens when the backend is
 * <em>broken</em>, and a fake that throws on demand reproduces that far more precisely — and far more
 * quickly — than partitioning a real container. The integration test in {@code RedisRateLimiterIT}
 * covers the happy path against a real Redis; this covers what a real Redis will not do on request.
 */
class ResilientRateLimiterTest {

    private static final RateLimitPolicy POLICY = RateLimitPolicy.of(10, Duration.ofSeconds(1));

    /** A stand-in backend whose availability the test controls. */
    private static final class FlakyBackend implements RateLimiter {
        private final AtomicBoolean up = new AtomicBoolean(true);
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public RateLimitDecision tryAcquire(String key, long permits) {
            calls.incrementAndGet();
            if (!up.get()) {
                throw new IllegalStateException("simulated Redis outage");
            }
            return RateLimitDecision.allowed(POLICY.permits(), POLICY.permits() - permits);
        }

        @Override
        public RateLimitDecision peek(String key, long permits) {
            return tryAcquire(key, permits);
        }

        @Override
        public RateLimitPolicy policy() {
            return POLICY;
        }
    }

    private RateLimiter localLimiter() {
        return Algorithm.GCRA.create(POLICY, new AdjustableTicker());
    }

    @Test
    @DisplayName("a healthy backend decides every request")
    void delegatesWhileHealthy() {
        FlakyBackend backend = new FlakyBackend();
        ResilientRateLimiter limiter = new ResilientRateLimiter(backend, localLimiter());

        for (int i = 0; i < 100; i++) {
            assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        }

        assertThat(backend.calls.get()).isEqualTo(100);
        assertThat(limiter.degraded()).isFalse();
        assertThat(limiter.degradedDecisionCount()).isZero();
    }

    @Test
    @DisplayName("a failing backend degrades to the local limiter rather than throwing")
    void degradesToLocalOnFailure() {
        FlakyBackend backend = new FlakyBackend();
        ResilientRateLimiter limiter =
                new ResilientRateLimiter(backend, localLimiter(), FailureMode.LOCAL_FALLBACK, 3, Duration.ofSeconds(10));

        backend.up.set(false);

        // The local limiter's own budget is 10, so the first 10 are allowed and the rest refused —
        // proof the decision really came from the fallback, not from failing open.
        int allowed = 0;
        for (int i = 0; i < 50; i++) {
            if (limiter.tryAcquire("alice").allowed()) {
                allowed++;
            }
        }

        assertThat(allowed).isEqualTo(POLICY.permits());
        assertThat(limiter.degradedDecisionCount()).isEqualTo(50);
    }

    @Test
    @DisplayName("the breaker opens after the threshold and stops calling the backend")
    void breakerOpensAndStopsCallingTheBackend() {
        FlakyBackend backend = new FlakyBackend();
        ResilientRateLimiter limiter =
                new ResilientRateLimiter(backend, localLimiter(), FailureMode.LOCAL_FALLBACK, 3, Duration.ofMinutes(1));

        backend.up.set(false);
        for (int i = 0; i < 100; i++) {
            limiter.tryAcquire("alice");
        }

        assertThat(limiter.degraded()).isTrue();
        assertThat(backend.calls.get())
                .as("once open, the breaker must stop adding load to a struggling backend")
                .isEqualTo(3);
        assertThat(limiter.backendFailureCount()).isEqualTo(3);
    }

    @Test
    @DisplayName("intermittent failures do not open the breaker — only consecutive ones do")
    void isolatedFailuresDoNotOpenTheBreaker() {
        FlakyBackend backend = new FlakyBackend();
        ResilientRateLimiter limiter =
                new ResilientRateLimiter(backend, localLimiter(), FailureMode.LOCAL_FALLBACK, 3, Duration.ofMinutes(1));

        // Fail, recover, fail, recover — never three in a row.
        for (int i = 0; i < 20; i++) {
            backend.up.set(i % 2 == 0);
            limiter.tryAcquire("alice");
        }

        assertThat(limiter.degraded())
                .as("a breaker that flaps on isolated timeouts is worse than no breaker")
                .isFalse();
    }

    @Test
    @DisplayName("the breaker probes after its window and closes when the backend recovers")
    void breakerClosesAfterRecovery() throws Exception {
        FlakyBackend backend = new FlakyBackend();
        ResilientRateLimiter limiter =
                new ResilientRateLimiter(backend, localLimiter(), FailureMode.LOCAL_FALLBACK, 2, Duration.ofMillis(50));

        backend.up.set(false);
        limiter.tryAcquire("alice");
        limiter.tryAcquire("alice");
        assertThat(limiter.degraded()).isTrue();

        backend.up.set(true);
        // The breaker reads System.nanoTime directly, so this is the one place a real wait is needed.
        Thread.sleep(80);

        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        assertThat(limiter.degraded()).isFalse();
    }

    @Test
    @DisplayName("FAIL_OPEN allows everything when the backend is down")
    void failOpenAllowsEverything() {
        FlakyBackend backend = new FlakyBackend();
        ResilientRateLimiter limiter =
                new ResilientRateLimiter(backend, localLimiter(), FailureMode.FAIL_OPEN, 1, Duration.ofMinutes(1));

        backend.up.set(false);

        for (int i = 0; i < 1_000; i++) {
            assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        }
    }

    @Test
    @DisplayName("FAIL_CLOSED refuses everything when the backend is down")
    void failClosedRefusesEverything() {
        FlakyBackend backend = new FlakyBackend();
        ResilientRateLimiter limiter =
                new ResilientRateLimiter(backend, localLimiter(), FailureMode.FAIL_CLOSED, 1, Duration.ofMinutes(1));

        backend.up.set(false);

        RateLimitDecision decision = limiter.tryAcquire("alice");
        assertThat(decision.denied()).isTrue();
        assertThat(decision.retryAfter())
                .as("a fail-closed refusal must still tell the client when to come back")
                .isPositive();
    }

    @Test
    @DisplayName("the local fallback is kept warm while the backend is healthy")
    void localFallbackIsWarmedDuringNormalOperation() {
        FlakyBackend backend = new FlakyBackend();
        RateLimiter local = localLimiter();
        ResilientRateLimiter limiter =
                new ResilientRateLimiter(backend, local, FailureMode.LOCAL_FALLBACK, 1, Duration.ofMinutes(1));

        // Warming is a peek, so it must not consume: the fallback has to be able to make real
        // decisions the moment it takes over, not arrive already exhausted.
        for (int i = 0; i < 100; i++) {
            limiter.tryAcquire("alice");
        }

        backend.up.set(false);
        assertThat(limiter.tryAcquire("alice").allowed())
                .as("warming must not have drained the fallback's budget")
                .isTrue();
    }

    @Test
    void rejectsAnInvalidFailureThreshold() {
        assertThatThrownBy(() -> new ResilientRateLimiter(
                        new FlakyBackend(), localLimiter(), FailureMode.FAIL_OPEN, 0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("failureThreshold must be at least 1");
    }

    @Test
    void reportsTheBackendPolicy() {
        ResilientRateLimiter limiter = new ResilientRateLimiter(new FlakyBackend(), localLimiter());

        assertThat(limiter.policy()).isEqualTo(POLICY);
    }
}
