package io.ratelimit4j.core;

import java.time.Duration;
import java.util.Objects;

/**
 * The outcome of a single rate limit check.
 *
 * <p>A decision carries enough information to populate the {@code RateLimit-*} response headers
 * described by the IETF {@code draft-ietf-httpapi-ratelimit-headers} draft, so callers never have to
 * re-derive remaining quota or retry timing themselves.
 *
 * @param allowed whether the permits were granted
 * @param limit the policy's sustained permit budget, echoed back for header emission
 * @param remaining permits still available at decision time; never negative
 * @param retryAfter how long the caller should wait before the request would succeed. {@link
 *     Duration#ZERO} when {@code allowed} is true.
 */
public record RateLimitDecision(boolean allowed, long limit, long remaining, Duration retryAfter) {

    public RateLimitDecision {
        Objects.requireNonNull(retryAfter, "retryAfter");
        if (remaining < 0) {
            throw new IllegalArgumentException("remaining must not be negative, got " + remaining);
        }
        if (allowed && !retryAfter.isZero()) {
            throw new IllegalArgumentException("an allowed decision cannot carry a retryAfter delay");
        }
    }

    public static RateLimitDecision allowed(long limit, long remaining) {
        return new RateLimitDecision(true, limit, remaining, Duration.ZERO);
    }

    public static RateLimitDecision denied(long limit, long remaining, Duration retryAfter) {
        return new RateLimitDecision(false, limit, remaining, retryAfter);
    }

    /**
     * @return {@code true} when the request was rejected — the negation of {@link #allowed()}, spelled
     *     out because {@code if (!decision.allowed())} reads poorly at call sites.
     */
    public boolean denied() {
        return !allowed;
    }

    /**
     * @return {@code Retry-After} in whole seconds, rounded up so a caller who honours it exactly is
     *     never rejected a second time for being a few milliseconds early.
     */
    public long retryAfterSeconds() {
        long nanos = retryAfter.toNanos();
        if (nanos <= 0) {
            return 0;
        }
        return (nanos + 999_999_999L) / 1_000_000_000L;
    }
}
