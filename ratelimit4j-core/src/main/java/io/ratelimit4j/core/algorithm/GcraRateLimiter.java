package io.ratelimit4j.core.algorithm;

import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.store.KeyedStateStore;
import io.ratelimit4j.core.store.Transition;
import io.ratelimit4j.core.time.Ticker;
import java.time.Duration;
import java.util.Objects;

/**
 * GCRA — the Generic Cell Rate Algorithm, borrowed from ATM traffic shaping.
 *
 * <p>Behaviourally equivalent to a token bucket, but it stores <em>one long</em> instead of a
 * token count plus a timestamp, and does its arithmetic entirely in integers.
 *
 * <h2>How it works</h2>
 *
 * The state is a single "theoretical arrival time" (TAT): the instant at which the bucket would next
 * be completely empty. Each permit costs one emission interval {@code T = window/permits}. A request
 * is allowed when the TAT it would produce is no further ahead than the delay tolerance
 * {@code τ = T × burst} — that is, when the client is not further ahead of schedule than its burst
 * allowance permits.
 *
 * <pre>
 *   tat'  = max(tat, now) + permits × T
 *   allow ⟺ tat' − τ ≤ now
 * </pre>
 *
 * <h2>Why prefer it over a token bucket</h2>
 *
 * <ul>
 *   <li><strong>No floating point.</strong> A token bucket's fractional token count drifts, however
 *       slightly; GCRA's arithmetic is exact, so its behaviour is exactly reproducible.
 *   <li><strong>Half the state.</strong> One {@code long} per key rather than a double and a long.
 *       At millions of keys that difference is real, and it is why Redis implementations of rate
 *       limiting (including the well-known {@code redis-cell} module) use GCRA.
 *   <li><strong>Trivially atomic.</strong> A single 64-bit value is a single CAS, or a single Redis
 *       {@code SET}, with no multi-field consistency to preserve.
 * </ul>
 *
 * <p>The cost is that it is harder to read: "remaining" has to be reconstructed from how far the TAT
 * sits behind {@code now}, rather than simply read off a counter.
 */
public final class GcraRateLimiter implements RateLimiter {

    private final RateLimitPolicy policy;
    private final Ticker ticker;
    private final KeyedStateStore<Tat> store;

    /** Emission interval: the spacing between permits at the sustained rate. */
    private final long intervalNanos;

    /** Delay tolerance: how far ahead of schedule a client may run. */
    private final long toleranceNanos;

    public GcraRateLimiter(RateLimitPolicy policy, Ticker ticker) {
        this(policy, ticker, Limiters.defaultStore(policy, ticker, now -> new Tat(now)));
    }

    public GcraRateLimiter(RateLimitPolicy policy, Ticker ticker, KeyedStateStore<Tat> store) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        this.store = Objects.requireNonNull(store, "store");
        this.intervalNanos = policy.nanosPerPermit();
        this.toleranceNanos = toleranceNanos(intervalNanos, policy.burst());
    }

    @Override
    public RateLimitDecision tryAcquire(String key, long permits) {
        Limiters.checkPermits(permits);
        if (permits > policy.burst()) {
            return RateLimitDecision.denied(policy.permits(), 0, Duration.ZERO);
        }
        long now = ticker.nanoTime();
        return store.apply(key, now, tat -> decide(tat, now, permits, true));
    }

    @Override
    public RateLimitDecision peek(String key, long permits) {
        Limiters.checkPermits(permits);
        if (permits > policy.burst()) {
            return RateLimitDecision.denied(policy.permits(), 0, Duration.ZERO);
        }
        long now = ticker.nanoTime();
        return store.apply(key, now, tat -> decide(tat, now, permits, false));
    }

    Transition<Tat, RateLimitDecision> decide(Tat state, long now, long permits, boolean consume) {
        // A TAT in the past means the bucket has been idle and is full; clamp so idle time beyond the
        // burst allowance does not accrue credit forever.
        long tat = Math.max(state.tatNanos(), now);
        long newTat = saturatedAdd(tat, intervalNanos * permits);
        long allowedAt = saturatedSub(newTat, toleranceNanos);

        if (allowedAt <= now) {
            Tat next = consume ? new Tat(newTat) : state;
            return Transition.of(next, RateLimitDecision.allowed(policy.permits(), remaining(next, now)));
        }
        return Transition.of(
                state,
                RateLimitDecision.denied(
                        policy.permits(), remaining(state, now), Duration.ofNanos(allowedAt - now)));
    }

    /**
     * Reconstructs the token-bucket view of the state: how many more permits could be spent right now.
     */
    private long remaining(Tat state, long now) {
        long headroom = saturatedSub(saturatedAdd(now, toleranceNanos), Math.max(state.tatNanos(), now));
        return Math.max(0, headroom / intervalNanos);
    }

    /**
     * Delay tolerance, saturating instead of wrapping.
     *
     * <p>{@code interval × burst} overflows for pathological policies — a burst in the billions on a
     * slow rate. Wrapping would turn a huge allowance into a negative one and reject everything, which
     * is the worst possible failure mode for a limiter. Saturating turns it into "effectively
     * unlimited", which is what the caller asked for.
     */
    private static long toleranceNanos(long intervalNanos, long burst) {
        try {
            return Math.multiplyExact(intervalNanos, burst);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE / 4;
        }
    }

    /** {@code a + b}, clamped to the long range rather than wrapping. */
    private static long saturatedAdd(long a, long b) {
        long sum = a + b;
        // Overflow iff the operands share a sign that the result does not.
        if (((a ^ sum) & (b ^ sum)) < 0) {
            return a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        return sum;
    }

    /** {@code a - b}, clamped to the long range rather than wrapping. */
    private static long saturatedSub(long a, long b) {
        long diff = a - b;
        // Overflow iff the operands differ in sign and the result matches the subtrahend's.
        if (((a ^ b) & (a ^ diff)) < 0) {
            return a < 0 ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        return diff;
    }

    @Override
    public RateLimitPolicy policy() {
        return policy;
    }

    /**
     * @param tatNanos the theoretical arrival time — when the bucket would next be empty
     */
    public record Tat(long tatNanos) {}
}
