package io.ratelimit4j.core.algorithm;

import static org.assertj.core.api.Assertions.assertThat;

import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.time.AdjustableTicker;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins down how each window-based algorithm behaves across a window boundary.
 *
 * <p>This is the difference that actually decides which one to deploy, and it is the one most easily
 * lost in a refactor, because every algorithm passes a naive single-window test. Asserting the
 * boundary spike as an explicit, expected property of the fixed window — rather than leaving it
 * undocumented — is the point of this class.
 */
class WindowBoundaryBehaviourTest {

    private static final long LIMIT = 100;
    private static final Duration WINDOW = Duration.ofSeconds(1);

    /**
     * Drives a key right up to the end of its first window, then just past the boundary.
     *
     * <p>The setup matters more than it looks. A window is anchored at the key's <em>first</em>
     * request, not at some absolute epoch, so the boundary cannot be reached by advancing the clock
     * before the key exists. One request is spent to anchor the window at a known instant; the rest of
     * the budget is spent just before that window closes; then the clock steps 2ms across the boundary.
     * That puts {@code LIMIT - 1} requests and whatever the second burst is granted within a 2ms span.
     *
     * @return permits granted in the burst after the boundary
     */
    private long burstAcrossBoundary(Algorithm algorithm, AdjustableTicker ticker) {
        RateLimiter limiter = algorithm.create(RateLimitPolicy.of(LIMIT, WINDOW), ticker);

        // Anchor the window at a known instant.
        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();

        // Spend the rest of the budget 1ms before the window closes.
        ticker.advance(WINDOW.minusMillis(1));
        assertThat(grantsFor(limiter, LIMIT)).isEqualTo(LIMIT - 1);
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();

        // Step across the boundary.
        ticker.advance(Duration.ofMillis(2));
        return grantsFor(limiter, LIMIT);
    }

    @Test
    @DisplayName("fixed window admits nearly 2x the limit in 2ms across a boundary")
    void fixedWindowSpikesAtTheBoundary() {
        long afterBoundary = burstAcrossBoundary(Algorithm.FIXED_WINDOW, new AdjustableTicker());

        assertThat(afterBoundary)
                .as("the counter reset, so a whole second budget is available immediately")
                .isEqualTo(LIMIT);
        assertThat(LIMIT - 1 + afterBoundary)
                .as("%d requests inside 2ms, all within a %d-per-second policy", LIMIT - 1 + afterBoundary, LIMIT)
                .isEqualTo(2 * LIMIT - 1);
    }

    @Test
    @DisplayName("sliding window counter refuses the same spike")
    void slidingWindowCounterSuppressesTheBoundarySpike() {
        long afterBoundary = burstAcrossBoundary(Algorithm.SLIDING_WINDOW_COUNTER, new AdjustableTicker());

        assertThat(afterBoundary)
                .as("the previous window is still weighted in at 99.9%%, leaving no room")
                .isZero();
    }

    @Test
    @DisplayName("sliding window log admits exactly the one request that aged out, and no more")
    void slidingWindowLogIsExactAtTheBoundary() {
        long afterBoundary = burstAcrossBoundary(Algorithm.SLIDING_WINDOW_LOG, new AdjustableTicker());

        // Only the anchoring request is older than a window at this point; the other 99 were sent 2ms
        // ago and still count. An exact algorithm releases precisely one slot, not a whole budget.
        assertThat(afterBoundary)
                .as("exactly one timestamp has aged out of the trailing window")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("the sliding counter's estimate tracks the previous window's decay linearly")
    void slidingCounterDecaysTheWeightLinearly() {
        AdjustableTicker ticker = new AdjustableTicker();
        RateLimiter limiter =
                Algorithm.SLIDING_WINDOW_COUNTER.create(RateLimitPolicy.of(LIMIT, WINDOW), ticker);

        grantsFor(limiter, LIMIT); // Fill window 0 completely.
        ticker.advance(WINDOW); // Roll: previous = 100, current = 0.

        // Half a window in, half the previous window's 100 still counts, leaving 50.
        ticker.advance(WINDOW.dividedBy(2));

        assertThat(grantsFor(limiter, LIMIT))
                .as("half the previous window should have aged out")
                .isBetween(49L, 51L);
    }

    @Test
    @DisplayName("the sliding log ages entries out one at a time, exactly a window after each")
    void slidingLogReleasesPermitsIndividually() {
        AdjustableTicker ticker = new AdjustableTicker();
        RateLimiter limiter = Algorithm.SLIDING_WINDOW_LOG.create(RateLimitPolicy.of(3, WINDOW), ticker);

        limiter.tryAcquire("alice");
        ticker.advance(Duration.ofMillis(100));
        limiter.tryAcquire("alice");
        ticker.advance(Duration.ofMillis(100));
        limiter.tryAcquire("alice");
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();

        // 1s after the first request, exactly one slot frees up.
        ticker.advance(WINDOW.minusMillis(200).plusMillis(1));
        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        assertThat(limiter.tryAcquire("alice").denied())
                .as("only the first entry has aged out; the other two still count")
                .isTrue();
    }

    private long grantsFor(RateLimiter limiter, long attempts) {
        long granted = 0;
        for (int i = 0; i < attempts; i++) {
            if (limiter.tryAcquire("alice").allowed()) {
                granted++;
            }
        }
        return granted;
    }
}
