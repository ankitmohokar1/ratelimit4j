package io.ratelimit4j.core.algorithm;

import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.store.ConcurrentKeyedStateStore;
import io.ratelimit4j.core.store.KeyedStateStore;
import io.ratelimit4j.core.time.Ticker;
import java.util.function.LongFunction;

/**
 * Shared construction and validation helpers for the algorithm implementations.
 */
final class Limiters {

    /**
     * Keys are kept for twice their policy window, floored at a minute.
     *
     * <p>Twice the window is the smallest safe multiple: at exactly one window a key is only just
     * indistinguishable from fresh, and clock granularity or a slow sweep could evict it a moment too
     * early. The one-minute floor stops a sub-second policy from thrashing its map.
     */
    static long defaultRetentionNanos(RateLimitPolicy policy) {
        return Math.max(policy.window().toNanos() * 2, java.time.Duration.ofMinutes(1).toNanos());
    }

    /**
     * A million keys is roughly 100 MB of state for the heaviest algorithm here, which is a bound
     * worth having by default and large enough that legitimate traffic will not reach it.
     */
    static final int DEFAULT_MAX_KEYS = 1_000_000;

    static <S> KeyedStateStore<S> defaultStore(RateLimitPolicy policy, Ticker ticker, LongFunction<S> seed) {
        return new ConcurrentKeyedStateStore<>(seed, ticker, defaultRetentionNanos(policy), DEFAULT_MAX_KEYS);
    }

    static void checkPermits(long permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be positive, got " + permits);
        }
    }

    private Limiters() {}
}
