package io.ratelimit4j.core.algorithm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.time.AdjustableTicker;
import java.time.Duration;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Invariants that every algorithm must satisfy, run against all of them.
 *
 * <p>The point of a shared contract suite is that adding a seventh algorithm costs one line in
 * {@link Algorithm} and immediately inherits every guarantee asserted here. It also stops the
 * algorithms drifting apart on the details clients actually depend on — whether {@code remaining} is
 * ever negative, whether {@code retryAfter} is honest, whether keys leak into one another.
 *
 * <p>Bounds are stated loosely enough to hold for the approximate algorithms too. Behaviour specific
 * to one algorithm — the fixed window's boundary spike, the log's exactness — is asserted in that
 * algorithm's own test, not here.
 */
class RateLimiterContractTest {

    private static final long PERMITS = 20;
    private static final Duration WINDOW = Duration.ofSeconds(1);

    private RateLimiter limiter(Algorithm algorithm, AdjustableTicker ticker) {
        return algorithm.create(RateLimitPolicy.of(PERMITS, WINDOW), ticker);
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @DisplayName("a fresh key can spend its full budget and no more")
    void freshKeySpendsExactlyItsBudget(Algorithm algorithm) {
        RateLimiter limiter = limiter(algorithm, new AdjustableTicker());

        long granted = 0;
        for (int i = 0; i < PERMITS * 2; i++) {
            if (limiter.tryAcquire("alice").allowed()) {
                granted++;
            }
        }

        assertThat(granted).isEqualTo(PERMITS);
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @DisplayName("the long-run grant rate never exceeds the policy rate, plus at most one burst")
    void sustainedRateIsBounded(Algorithm algorithm) {
        AdjustableTicker ticker = new AdjustableTicker();
        RateLimiter limiter = limiter(algorithm, ticker);

        // Hammer for 10 windows, attempting far more than the policy allows.
        int windows = 10;
        long granted = 0;
        for (int w = 0; w < windows; w++) {
            for (int i = 0; i < PERMITS * 5; i++) {
                if (limiter.tryAcquire("alice").allowed()) {
                    granted++;
                }
                ticker.advanceNanos(WINDOW.toNanos() / (PERMITS * 5));
            }
        }

        // The +PERMITS slack is the initial burst, which every algorithm grants up front and which is
        // not part of the sustained rate.
        assertThat(granted)
                .as("%s granted %d over %d windows of %d", algorithm, granted, windows, PERMITS)
                .isLessThanOrEqualTo(PERMITS * windows + PERMITS);
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @DisplayName("an idle key recovers its full budget")
    void budgetRecoversAfterIdling(Algorithm algorithm) {
        AdjustableTicker ticker = new AdjustableTicker();
        RateLimiter limiter = limiter(algorithm, ticker);

        exhaust(limiter, "alice");
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();

        // Two windows, not one. For the sliding algorithms a single window is genuinely not enough:
        // traffic sent at T is still inside the trailing window at T + WINDOW, so it still counts.
        // Two windows is the weakest statement that holds for every algorithm here.
        ticker.advance(WINDOW.multipliedBy(2));

        long granted = 0;
        for (int i = 0; i < PERMITS; i++) {
            if (limiter.tryAcquire("alice").allowed()) {
                granted++;
            }
        }
        assertThat(granted)
                .as("%s should have recovered its whole budget after idling", algorithm)
                .isEqualTo(PERMITS);
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @DisplayName("remaining is never negative and never exceeds the limit")
    void remainingStaysInRange(Algorithm algorithm) {
        AdjustableTicker ticker = new AdjustableTicker();
        RateLimiter limiter = limiter(algorithm, ticker);
        Random random = new Random(20240917L);

        for (int i = 0; i < 500; i++) {
            RateLimitDecision decision = limiter.tryAcquire("alice", 1 + random.nextInt(3));
            assertThat(decision.remaining()).isBetween(0L, PERMITS);
            ticker.advanceNanos(random.nextInt((int) (WINDOW.toNanos() / 10)));
        }
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @DisplayName("honouring the advertised retryAfter always succeeds when nothing else competes")
    void retryAfterIsHonest(Algorithm algorithm) {
        AdjustableTicker ticker = new AdjustableTicker();
        RateLimiter limiter = limiter(algorithm, ticker);

        exhaust(limiter, "alice");
        RateLimitDecision denied = limiter.tryAcquire("alice");
        assertThat(denied.denied()).isTrue();
        assertThat(denied.retryAfter()).isPositive();

        ticker.advance(denied.retryAfter());

        assertThat(limiter.tryAcquire("alice").allowed())
                .as("%s advertised a %s wait that was not sufficient", algorithm, denied.retryAfter())
                .isTrue();
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @DisplayName("one key's traffic never consumes another key's budget")
    void keysAreIsolated(Algorithm algorithm) {
        RateLimiter limiter = limiter(algorithm, new AdjustableTicker());

        exhaust(limiter, "alice");

        assertThat(limiter.tryAcquire("bob").allowed()).isTrue();
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @DisplayName("peek does not consume")
    void peekIsNonDestructive(Algorithm algorithm) {
        RateLimiter limiter = limiter(algorithm, new AdjustableTicker());

        for (int i = 0; i < 50; i++) {
            assertThat(limiter.peek("alice", 1).allowed()).isTrue();
        }

        assertThat(limiter.tryAcquire("alice", PERMITS).allowed())
                .as("%s let peeks eat into the budget", algorithm)
                .isTrue();
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    void rejectsNonPositivePermits(Algorithm algorithm) {
        RateLimiter limiter = limiter(algorithm, new AdjustableTicker());

        assertThatThrownBy(() -> limiter.tryAcquire("alice", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter.tryAcquire("alice", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @DisplayName("a multi-permit request costs exactly as much as the same permits one at a time")
    void multiPermitMatchesRepeatedSinglePermit(Algorithm algorithm) {
        RateLimiter batched = limiter(algorithm, new AdjustableTicker());
        RateLimiter singly = limiter(algorithm, new AdjustableTicker());

        RateLimitDecision batchedDecision = batched.tryAcquire("alice", 5);
        RateLimitDecision singleDecision = null;
        for (int i = 0; i < 5; i++) {
            singleDecision = singly.tryAcquire("alice", 1);
        }

        assertThat(batchedDecision.allowed()).isTrue();
        assertThat(singleDecision).isNotNull();
        assertThat(batchedDecision.remaining()).isEqualTo(singleDecision.remaining());
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    void reportsThePolicyItEnforces(Algorithm algorithm) {
        RateLimiter limiter = limiter(algorithm, new AdjustableTicker());

        assertThat(limiter.policy().permits()).isEqualTo(PERMITS);
        assertThat(limiter.policy().window()).isEqualTo(WINDOW);
    }

    private void exhaust(RateLimiter limiter, String key) {
        for (int i = 0; i < PERMITS; i++) {
            limiter.tryAcquire(key);
        }
    }
}
