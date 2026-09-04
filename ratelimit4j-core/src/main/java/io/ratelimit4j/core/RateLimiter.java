package io.ratelimit4j.core;

/**
 * Decides whether a key may spend permits right now.
 *
 * <p>A "key" is whatever dimension you are limiting on: an API token, a client IP, a tenant id, or a
 * composite like {@code "tenant-42:POST /orders"}. The limiter attaches no meaning to it beyond
 * equality.
 *
 * <p><strong>Thread safety.</strong> All implementations in this library are safe for concurrent use
 * by many threads against the same or different keys, and the in-process ones are lock-free on the
 * hot path.
 *
 * <p><strong>Fairness.</strong> None of these limiters queue or block. A rejected caller is told how
 * long to wait; it is not put in line. That keeps the limiter O(1) and non-blocking, at the cost of
 * offering no ordering guarantee between competing callers.
 */
public interface RateLimiter {

    /**
     * Attempts to spend {@code permits} against {@code key}, consuming them if and only if the policy
     * allows.
     *
     * @param key the dimension being limited; must not be null
     * @param permits how many permits to spend; must be positive. Values above the policy's burst
     *     allowance can never be granted and are rejected immediately rather than waiting forever.
     * @return the decision, including remaining quota and retry timing
     */
    RateLimitDecision tryAcquire(String key, long permits);

    /**
     * Spends a single permit.
     */
    default RateLimitDecision tryAcquire(String key) {
        return tryAcquire(key, 1);
    }

    /**
     * Reports what {@link #tryAcquire} would return without consuming anything.
     *
     * <p>Used by {@link io.ratelimit4j.core.TieredRateLimiter} to avoid charging a permit to one tier
     * when a later tier is going to reject the request anyway. Note that peek-then-acquire is not
     * atomic: a concurrent caller can change the answer in between. See {@code TieredRateLimiter} for
     * what that costs in practice.
     */
    RateLimitDecision peek(String key, long permits);

    /**
     * @return the policy this limiter enforces
     */
    RateLimitPolicy policy();
}
