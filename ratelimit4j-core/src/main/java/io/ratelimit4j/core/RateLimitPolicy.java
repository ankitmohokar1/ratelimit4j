package io.ratelimit4j.core;

import java.time.Duration;
import java.util.Objects;

/**
 * How many permits a key may spend over a window, and how much of that budget may be spent at once.
 *
 * <p>{@code permits} and {@code window} together define the sustained rate. {@code burst} defines
 * the instantaneous allowance — the depth of a token bucket, or the delay tolerance of GCRA. The two
 * are separate on purpose: "1000 requests per hour" and "1000 requests, all in the first second, then
 * nothing for an hour" are very different guarantees, and only a burst parameter distinguishes them.
 *
 * <p>Algorithms that have no notion of burst ({@link io.ratelimit4j.core.algorithm.FixedWindowRateLimiter},
 * for example) ignore the field; the Javadoc on each implementation says which apply.
 *
 * @param permits sustained permits granted per window; must be positive
 * @param window the window over which {@code permits} are granted; must be positive
 * @param burst maximum permits available instantaneously; must be at least 1
 */
public record RateLimitPolicy(long permits, Duration window, long burst) {

    public RateLimitPolicy {
        Objects.requireNonNull(window, "window");
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be positive, got " + permits);
        }
        if (window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("window must be positive, got " + window);
        }
        if (burst < 1) {
            throw new IllegalArgumentException("burst must be at least 1, got " + burst);
        }
        if (window.toNanos() <= 0) {
            throw new IllegalArgumentException("window must be at least 1ns, got " + window);
        }
    }

    /**
     * A policy whose burst allowance equals its sustained budget — the common default, and what most
     * people mean by "100 requests per minute".
     */
    public static RateLimitPolicy of(long permits, Duration window) {
        return new RateLimitPolicy(permits, window, permits);
    }

    public static RateLimitPolicy perSecond(long permits) {
        return of(permits, Duration.ofSeconds(1));
    }

    public static RateLimitPolicy perMinute(long permits) {
        return of(permits, Duration.ofMinutes(1));
    }

    public static RateLimitPolicy perHour(long permits) {
        return of(permits, Duration.ofHours(1));
    }

    /**
     * @return a copy of this policy with a different burst allowance
     */
    public RateLimitPolicy withBurst(long newBurst) {
        return new RateLimitPolicy(permits, window, newBurst);
    }

    /**
     * @return the sustained rate in permits per nanosecond. Kept as a {@code double} because the
     *     realistic rates here (single digits per second up to millions per second) all sit far inside
     *     the range where a double's 53-bit mantissa is exact enough that accumulated error stays
     *     below the nanosecond resolution of the clock feeding it.
     */
    public double permitsPerNano() {
        return (double) permits / (double) window.toNanos();
    }

    /**
     * @return the average spacing between permits at the sustained rate — the emission interval in
     *     GCRA terms.
     */
    public long nanosPerPermit() {
        return Math.max(1L, window.toNanos() / permits);
    }
}
