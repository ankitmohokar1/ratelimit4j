package io.ratelimit4j.core.algorithm;

import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.time.Ticker;
import java.util.function.BiFunction;

/**
 * The algorithms this library implements, so configuration can name one as a string.
 *
 * <p>The comparison below is the short version of {@code docs/algorithms.md}.
 *
 * <table border="1">
 *   <caption>Choosing an algorithm</caption>
 *   <tr><th>Algorithm</th><th>State/key</th><th>Exact?</th><th>Bursts?</th><th>Use when</th></tr>
 *   <tr><td>GCRA</td><td>8 B</td><td>Yes</td><td>Yes</td><td>Default. Cheapest exact option.</td></tr>
 *   <tr><td>Token bucket</td><td>16 B</td><td>Yes</td><td>Yes</td><td>You want the textbook model.</td></tr>
 *   <tr><td>Leaky bucket</td><td>16 B</td><td>Yes</td><td>Yes</td><td>Dual of token bucket.</td></tr>
 *   <tr><td>Sliding window counter</td><td>24 B</td><td>~1% error</td><td>No</td><td>Large limits, smooth.</td></tr>
 *   <tr><td>Sliding window log</td><td>8N B</td><td>Yes</td><td>No</td><td>Small, high-value limits.</td></tr>
 *   <tr><td>Fixed window</td><td>16 B</td><td>2x at boundary</td><td>No</td><td>Calendar-aligned quotas.</td></tr>
 * </table>
 */
public enum Algorithm {

    /** {@link GcraRateLimiter} — the default: exact, integer-only, 8 bytes per key. */
    GCRA(GcraRateLimiter::new),

    /** {@link TokenBucketRateLimiter} — the textbook burst-tolerant limiter. */
    TOKEN_BUCKET(TokenBucketRateLimiter::new),

    /** {@link LeakyBucketRateLimiter} — the token bucket's dual. */
    LEAKY_BUCKET(LeakyBucketRateLimiter::new),

    /** {@link SlidingWindowCounterRateLimiter} — smooth, cheap, approximate. */
    SLIDING_WINDOW_COUNTER(SlidingWindowCounterRateLimiter::new),

    /** {@link SlidingWindowLogRateLimiter} — exact, O(N) memory. */
    SLIDING_WINDOW_LOG(SlidingWindowLogRateLimiter::new),

    /** {@link FixedWindowRateLimiter} — simplest, spikes at boundaries. */
    FIXED_WINDOW(FixedWindowRateLimiter::new);

    private final BiFunction<RateLimitPolicy, Ticker, RateLimiter> factory;

    Algorithm(BiFunction<RateLimitPolicy, Ticker, RateLimiter> factory) {
        this.factory = factory;
    }

    /**
     * Builds an in-process limiter enforcing {@code policy}.
     */
    public RateLimiter create(RateLimitPolicy policy, Ticker ticker) {
        return factory.apply(policy, ticker);
    }

    /**
     * Builds an in-process limiter driven by the system clock.
     */
    public RateLimiter create(RateLimitPolicy policy) {
        return create(policy, Ticker.system());
    }
}
