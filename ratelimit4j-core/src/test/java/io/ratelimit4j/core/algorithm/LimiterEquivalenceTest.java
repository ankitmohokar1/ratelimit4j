package io.ratelimit4j.core.algorithm;

import static org.assertj.core.api.Assertions.assertThat;

import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.time.AdjustableTicker;
import java.time.Duration;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Asserts that algorithms claimed to be equivalent really are, over randomised traces.
 *
 * <p>The token bucket, the leaky bucket (metered) and GCRA are three descriptions of one thing. That
 * claim is made in each class's Javadoc, and this is what keeps it true: a difference in refill
 * rounding or boundary handling would show up here as a diverging decision, rather than as a subtle
 * behaviour change nobody notices.
 *
 * <p>Each trace replays the same randomised sequence of request sizes and time gaps against every
 * implementation and requires identical allow/deny decisions at every step. Several seeds are used so
 * a passing run is not an accident of one particular trace.
 */
class LimiterEquivalenceTest {

    @ParameterizedTest
    @ValueSource(longs = {1L, 7L, 42L, 1_337L, 20_240_917L})
    @DisplayName("token bucket and leaky bucket agree decision-for-decision")
    void tokenBucketMatchesLeakyBucket(long seed) {
        assertAgreeOverRandomTrace(Algorithm.TOKEN_BUCKET, Algorithm.LEAKY_BUCKET, seed);
    }

    @ParameterizedTest
    @ValueSource(longs = {1L, 7L, 42L, 1_337L, 20_240_917L})
    @DisplayName("GCRA agrees with the token bucket it is an integer reformulation of")
    void gcraMatchesTokenBucket(long seed) {
        assertAgreeOverRandomTrace(Algorithm.GCRA, Algorithm.TOKEN_BUCKET, seed);
    }

    @Test
    @DisplayName("GCRA with burst=1 spaces admissions evenly, as a traffic shaper would")
    void gcraWithUnitBurstShapesTraffic() {
        AdjustableTicker ticker = new AdjustableTicker();
        RateLimitPolicy shaped = RateLimitPolicy.of(10, Duration.ofSeconds(1)).withBurst(1);
        RateLimiter limiter = Algorithm.GCRA.create(shaped, ticker);

        // One permit every 100ms, and no bursting whatsoever.
        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();

        ticker.advance(Duration.ofMillis(100));
        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();

        // Even after idling, only one permit is available — that is what "no burst" means.
        ticker.advance(Duration.ofSeconds(10));
        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();
    }

    private void assertAgreeOverRandomTrace(Algorithm left, Algorithm right, long seed) {
        RateLimitPolicy policy = RateLimitPolicy.of(50, Duration.ofSeconds(1)).withBurst(50);

        // Separate tickers, advanced identically, so neither implementation can influence the other.
        AdjustableTicker leftTicker = new AdjustableTicker();
        AdjustableTicker rightTicker = new AdjustableTicker();
        RateLimiter leftLimiter = left.create(policy, leftTicker);
        RateLimiter rightLimiter = right.create(policy, rightTicker);

        Random random = new Random(seed);
        for (int step = 0; step < 2_000; step++) {
            long permits = 1 + random.nextInt(5);
            RateLimitDecision leftDecision = leftLimiter.tryAcquire("alice", permits);
            RateLimitDecision rightDecision = rightLimiter.tryAcquire("alice", permits);

            assertThat(leftDecision.allowed())
                    .as("step %d (seed %d): %s said %s, %s said %s for %d permits",
                            step, seed, left, leftDecision.allowed(), right, rightDecision.allowed(), permits)
                    .isEqualTo(rightDecision.allowed());

            // Gaps span three orders of magnitude, so the trace covers both back-to-back bursts and
            // idle periods long enough to refill completely.
            long gapNanos = random.nextInt(50_000_000);
            leftTicker.advanceNanos(gapNanos);
            rightTicker.advanceNanos(gapNanos);
        }
    }
}
