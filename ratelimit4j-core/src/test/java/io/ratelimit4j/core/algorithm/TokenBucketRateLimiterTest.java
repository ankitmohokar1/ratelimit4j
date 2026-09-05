package io.ratelimit4j.core.algorithm;

import static org.assertj.core.api.Assertions.assertThat;

import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.time.AdjustableTicker;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TokenBucketRateLimiterTest {

    private AdjustableTicker ticker;

    @BeforeEach
    void setUp() {
        ticker = new AdjustableTicker();
    }

    private TokenBucketRateLimiter limiter(long permits, Duration window) {
        return new TokenBucketRateLimiter(RateLimitPolicy.of(permits, window), ticker);
    }

    @Nested
    class WhenTheBucketIsFull {

        @Test
        @DisplayName("a cold key starts with a full bucket and can spend the whole burst at once")
        void coldKeyStartsFull() {
            TokenBucketRateLimiter limiter = limiter(10, Duration.ofSeconds(1));

            for (int i = 0; i < 10; i++) {
                assertThat(limiter.tryAcquire("alice").allowed())
                        .as("request %d of the initial burst", i + 1)
                        .isTrue();
            }
            assertThat(limiter.tryAcquire("alice").denied()).isTrue();
        }

        @Test
        void remainingCountsDownAsPermitsAreSpent() {
            TokenBucketRateLimiter limiter = limiter(5, Duration.ofSeconds(1));

            assertThat(limiter.tryAcquire("alice").remaining()).isEqualTo(4);
            assertThat(limiter.tryAcquire("alice").remaining()).isEqualTo(3);
            assertThat(limiter.tryAcquire("alice", 3).remaining()).isZero();
        }
    }

    @Nested
    class WhenTheBucketIsEmpty {

        @Test
        @DisplayName("refill is proportional to elapsed time, not to the number of requests")
        void refillsProportionallyToElapsedTime() {
            TokenBucketRateLimiter limiter = limiter(10, Duration.ofSeconds(1));
            drain(limiter, "alice", 10);

            ticker.advance(Duration.ofMillis(300)); // 30% of a window = 3 tokens.

            assertThat(limiter.tryAcquire("alice", 3).allowed()).isTrue();
            assertThat(limiter.tryAcquire("alice").denied()).isTrue();
        }

        @Test
        void retryAfterIsTheExactWaitForTheMissingTokens() {
            TokenBucketRateLimiter limiter = limiter(10, Duration.ofSeconds(1));
            drain(limiter, "alice", 10);

            RateLimitDecision denied = limiter.tryAcquire("alice", 5);

            // Five tokens at ten per second is half a second.
            assertThat(denied.denied()).isTrue();
            assertThat(denied.retryAfter()).isEqualTo(Duration.ofMillis(500));
        }

        @Test
        @DisplayName("waiting exactly the advertised retryAfter is enough to succeed")
        void honouringRetryAfterSucceeds() {
            TokenBucketRateLimiter limiter = limiter(10, Duration.ofSeconds(1));
            drain(limiter, "alice", 10);
            RateLimitDecision denied = limiter.tryAcquire("alice", 4);

            ticker.advance(denied.retryAfter());

            assertThat(limiter.tryAcquire("alice", 4).allowed()).isTrue();
        }
    }

    @Test
    @DisplayName("the bucket never fills past its burst, however long the key idles")
    void idleTimeDoesNotAccrueUnboundedCredit() {
        TokenBucketRateLimiter limiter = limiter(10, Duration.ofSeconds(1));
        drain(limiter, "alice", 10);

        ticker.advance(Duration.ofHours(1));

        drain(limiter, "alice", 10);
        assertThat(limiter.tryAcquire("alice").denied())
                .as("an hour of idling must not bank more than one bucket")
                .isTrue();
    }

    @Test
    void keysAreLimitedIndependently() {
        TokenBucketRateLimiter limiter = limiter(2, Duration.ofSeconds(1));

        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();

        assertThat(limiter.tryAcquire("bob").allowed())
                .as("bob's budget is his own")
                .isTrue();
    }

    @Test
    @DisplayName("a request larger than the burst is refused outright, not made to wait forever")
    void requestsLargerThanBurstFailFast() {
        TokenBucketRateLimiter limiter = limiter(10, Duration.ofSeconds(1));

        RateLimitDecision decision = limiter.tryAcquire("alice", 11);

        assertThat(decision.denied()).isTrue();
        assertThat(decision.retryAfter())
                .as("no amount of waiting makes an 11-permit request fit a 10-token bucket")
                .isEqualTo(Duration.ZERO);
    }

    @Test
    void peekReportsTheDecisionWithoutSpendingAnything() {
        TokenBucketRateLimiter limiter = limiter(3, Duration.ofSeconds(1));

        assertThat(limiter.peek("alice", 3).allowed()).isTrue();
        assertThat(limiter.peek("alice", 3).allowed()).isTrue();
        assertThat(limiter.peek("alice", 3).remaining())
                .as("three peeks must leave the budget untouched")
                .isEqualTo(3);
        assertThat(limiter.tryAcquire("alice", 3).allowed()).isTrue();
    }

    @Test
    @DisplayName("a burst larger than the sustained rate is allowed once, then throttled to the rate")
    void burstAboveSustainedRate() {
        RateLimitPolicy bursty = RateLimitPolicy.of(10, Duration.ofSeconds(1)).withBurst(50);
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(bursty, ticker);

        drain(limiter, "alice", 50);
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();

        ticker.advance(Duration.ofSeconds(1)); // Refills at the sustained rate: 10, not 50.
        drain(limiter, "alice", 10);
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();
    }

    @Test
    @DisplayName("a ticker that does not advance cannot be used to mint tokens")
    void nonAdvancingClockGrantsNothingExtra() {
        TokenBucketRateLimiter limiter = limiter(5, Duration.ofSeconds(1));

        for (int i = 0; i < 100; i++) {
            limiter.tryAcquire("alice");
        }

        assertThat(limiter.tryAcquire("alice").denied()).isTrue();
    }

    private void drain(TokenBucketRateLimiter limiter, String key, int count) {
        for (int i = 0; i < count; i++) {
            assertThat(limiter.tryAcquire(key).allowed()).as("drain request %d", i + 1).isTrue();
        }
    }
}
