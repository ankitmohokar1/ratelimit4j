package io.ratelimit4j.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RateLimitPolicyTest {

    @Test
    @DisplayName("of() defaults burst to the sustained budget")
    void ofDefaultsBurstToPermits() {
        RateLimitPolicy policy = RateLimitPolicy.of(100, Duration.ofMinutes(1));

        assertThat(policy.permits()).isEqualTo(100);
        assertThat(policy.burst()).isEqualTo(100);
        assertThat(policy.window()).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void withBurstLeavesTheSustainedRateAlone() {
        RateLimitPolicy relaxed = RateLimitPolicy.perSecond(10).withBurst(50);

        assertThat(relaxed.permits()).isEqualTo(10);
        assertThat(relaxed.burst()).isEqualTo(50);
        assertThat(relaxed.permitsPerNano()).isEqualTo(RateLimitPolicy.perSecond(10).permitsPerNano());
    }

    @ParameterizedTest
    @ValueSource(longs = {0, -1, Long.MIN_VALUE})
    void rejectsNonPositivePermits(long permits) {
        assertThatThrownBy(() -> RateLimitPolicy.of(permits, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("permits must be positive");
    }

    @Test
    void rejectsAZeroWindow() {
        assertThatThrownBy(() -> RateLimitPolicy.of(10, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("window must be positive");
    }

    @Test
    void rejectsANegativeWindow() {
        assertThatThrownBy(() -> RateLimitPolicy.of(10, Duration.ofSeconds(-5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("window must be positive");
    }

    @Test
    void rejectsABurstBelowOne() {
        assertThatThrownBy(() -> new RateLimitPolicy(10, Duration.ofSeconds(1), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("burst must be at least 1");
    }

    @Test
    @DisplayName("nanosPerPermit never rounds down to zero, which would mean an unlimited rate")
    void nanosPerPermitFloorsAtOne() {
        // A billion permits per nanosecond-scale window would divide to zero in integer arithmetic.
        RateLimitPolicy absurd = RateLimitPolicy.of(1_000_000_000L, Duration.ofNanos(1));

        assertThat(absurd.nanosPerPermit()).isEqualTo(1);
    }

    @Test
    void permitsPerNanoMatchesTheDeclaredRate() {
        RateLimitPolicy thousandPerSecond = RateLimitPolicy.perSecond(1_000);

        // 1000 permits / 1e9 ns = 1e-6 permits per nanosecond.
        assertThat(thousandPerSecond.permitsPerNano()).isEqualTo(1e-6);
    }
}
