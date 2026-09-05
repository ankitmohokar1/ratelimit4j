package io.ratelimit4j.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.ratelimit4j.core.algorithm.Algorithm;
import io.ratelimit4j.core.time.AdjustableTicker;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TieredRateLimiterTest {

    private final AdjustableTicker ticker = new AdjustableTicker();

    private RateLimiter tier(long permits, Duration window) {
        return Algorithm.GCRA.create(RateLimitPolicy.of(permits, window), ticker);
    }

    @Test
    @DisplayName("a request must satisfy every tier")
    void allTiersMustAgree() {
        TieredRateLimiter limiter =
                TieredRateLimiter.of(tier(5, Duration.ofSeconds(1)), tier(100, Duration.ofHours(1)));

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        }

        assertThat(limiter.tryAcquire("alice").denied())
                .as("the per-second tier is exhausted even though the hourly one is not")
                .isTrue();
    }

    @Test
    @DisplayName("the slower tier still binds after the faster one has refilled many times")
    void theSlowerTierEventuallyBinds() {
        TieredRateLimiter limiter =
                TieredRateLimiter.of(tier(5, Duration.ofSeconds(1)), tier(20, Duration.ofHours(1)));

        long granted = 0;
        for (int second = 0; second < 100; second++) {
            for (int i = 0; i < 10; i++) {
                if (limiter.tryAcquire("alice").allowed()) {
                    granted++;
                }
            }
            ticker.advance(Duration.ofSeconds(1));
        }

        assertThat(granted)
                .as("100 seconds at 5/s would be 500, but the hourly tier caps it near 20")
                .isBetween(20L, 22L);
    }

    @Test
    @DisplayName("a tier that would allow is not charged when a later tier refuses")
    void aRefusedRequestDoesNotDrainEarlierTiers() {
        RateLimiter perSecond = tier(10, Duration.ofSeconds(1));
        RateLimiter perHour = tier(2, Duration.ofHours(1));
        TieredRateLimiter limiter = TieredRateLimiter.of(perSecond, perHour);

        // Exhaust the hourly tier through the tiered limiter.
        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();

        // Eight further attempts, all refused by the hourly tier.
        for (int i = 0; i < 8; i++) {
            assertThat(limiter.tryAcquire("alice").denied()).isTrue();
        }

        // The naive implementation would have charged all eight to the per-second tier, leaving it
        // empty. Peek-then-commit means it has spent only the two that were actually granted.
        assertThat(perSecond.peek("alice", 8).allowed())
                .as("the per-second tier should still have 8 of its 10 permits")
                .isTrue();
    }

    @Test
    void reportsTheHeadroomOfTheTightestTier() {
        TieredRateLimiter limiter =
                TieredRateLimiter.of(tier(100, Duration.ofSeconds(1)), tier(3, Duration.ofHours(1)));

        RateLimitDecision decision = limiter.tryAcquire("alice");

        assertThat(decision.allowed()).isTrue();
        assertThat(decision.remaining())
                .as("reporting the per-second tier's 99 would badly mislead the client")
                .isEqualTo(2);
    }

    @Test
    void peekConsumesNothingFromAnyTier() {
        RateLimiter perSecond = tier(5, Duration.ofSeconds(1));
        RateLimiter perHour = tier(5, Duration.ofHours(1));
        TieredRateLimiter limiter = TieredRateLimiter.of(perSecond, perHour);

        for (int i = 0; i < 20; i++) {
            assertThat(limiter.peek("alice", 1).allowed()).isTrue();
        }

        assertThat(limiter.tryAcquire("alice", 5).allowed()).isTrue();
    }

    @Test
    void keysRemainIndependentAcrossTiers() {
        TieredRateLimiter limiter =
                TieredRateLimiter.of(tier(2, Duration.ofSeconds(1)), tier(2, Duration.ofHours(1)));

        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();

        assertThat(limiter.tryAcquire("bob").allowed()).isTrue();
    }

    @Test
    @DisplayName("policy() reports the tier a client hits first in steady state")
    void policyReportsTheStrictestSustainedRate() {
        TieredRateLimiter limiter =
                TieredRateLimiter.of(tier(1000, Duration.ofSeconds(1)), tier(10, Duration.ofHours(1)));

        assertThat(limiter.policy().permits()).isEqualTo(10);
        assertThat(limiter.policy().window()).isEqualTo(Duration.ofHours(1));
    }

    @Test
    void rejectsAnEmptyTierList() {
        assertThatThrownBy(() -> new TieredRateLimiter(List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one tier");
    }

    @Test
    void exposesItsTiersInEvaluationOrder() {
        RateLimiter first = tier(5, Duration.ofSeconds(1));
        RateLimiter second = tier(50, Duration.ofMinutes(1));

        assertThat(TieredRateLimiter.of(first, second).tiers()).containsExactly(first, second);
    }
}
