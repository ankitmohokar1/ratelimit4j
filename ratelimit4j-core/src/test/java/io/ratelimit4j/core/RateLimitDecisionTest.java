package io.ratelimit4j.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RateLimitDecisionTest {

    @Test
    void allowedDecisionsCarryNoRetryDelay() {
        RateLimitDecision decision = RateLimitDecision.allowed(100, 42);

        assertThat(decision.allowed()).isTrue();
        assertThat(decision.denied()).isFalse();
        assertThat(decision.retryAfter()).isEqualTo(Duration.ZERO);
        assertThat(decision.retryAfterSeconds()).isZero();
    }

    @Test
    @DisplayName("an allowed decision with a retry delay is a contradiction and is rejected")
    void rejectsAllowedWithRetryAfter() {
        assertThatThrownBy(() -> new RateLimitDecision(true, 100, 0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowed decision cannot carry a retryAfter");
    }

    @Test
    void rejectsNegativeRemaining() {
        assertThatThrownBy(() -> new RateLimitDecision(false, 100, -1, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("remaining must not be negative");
    }

    @Test
    @DisplayName("Retry-After rounds up, so a client that obeys it exactly is not rejected again")
    void retryAfterSecondsRoundsUp() {
        assertThat(RateLimitDecision.denied(10, 0, Duration.ofMillis(1)).retryAfterSeconds()).isEqualTo(1);
        assertThat(RateLimitDecision.denied(10, 0, Duration.ofMillis(1_001)).retryAfterSeconds()).isEqualTo(2);
        assertThat(RateLimitDecision.denied(10, 0, Duration.ofSeconds(3)).retryAfterSeconds()).isEqualTo(3);
    }

    @Test
    void aNanosecondStillRoundsUpToAWholeSecond() {
        assertThat(RateLimitDecision.denied(10, 0, Duration.ofNanos(1)).retryAfterSeconds()).isEqualTo(1);
    }
}
