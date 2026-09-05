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
 * Leaky bucket, metered form: each permit adds to a bucket that drains at a constant rate, and a
 * request is refused when it would overflow.
 *
 * <h2>Its relationship to the token bucket</h2>
 *
 * They are the same algorithm viewed from opposite ends. The token bucket counts what remains and
 * refills upward; the leaky bucket counts what is used and drains downward. Substituting
 * {@code level = burst − tokens} turns either one into the other, and {@code LimiterEquivalenceTest}
 * asserts that the two implementations agree decision-for-decision over a randomised trace.
 *
 * <p>Both are included deliberately. Interview answers and design docs routinely present them as a
 * meaningful choice, and it is worth being able to show that, in the metered form, it is not one.
 *
 * <p>The genuine distinction is with the <em>queueing</em> leaky bucket, which is a traffic shaper
 * rather than a limiter: it admits the request and delays it until the bucket has drained enough,
 * producing a perfectly smooth output rate. That is a different contract — it blocks, it needs a
 * queue, and it can reorder — and it does not fit the non-blocking {@link RateLimiter} interface. If
 * output smoothing is what you need, {@link GcraRateLimiter} configured with {@code burst = 1} gives
 * the same evenly-spaced admission without the queue.
 *
 * <p>Capacity is {@code burst}; the drain rate is the policy's sustained rate.
 */
public final class LeakyBucketRateLimiter implements RateLimiter {

    private final RateLimitPolicy policy;
    private final Ticker ticker;
    private final KeyedStateStore<Bucket> store;
    private final double leakPerNano;

    public LeakyBucketRateLimiter(RateLimitPolicy policy, Ticker ticker) {
        this(policy, ticker, Limiters.defaultStore(policy, ticker, now -> new Bucket(0.0, now)));
    }

    public LeakyBucketRateLimiter(RateLimitPolicy policy, Ticker ticker, KeyedStateStore<Bucket> store) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        this.store = Objects.requireNonNull(store, "store");
        this.leakPerNano = policy.permitsPerNano();
    }

    @Override
    public RateLimitDecision tryAcquire(String key, long permits) {
        Limiters.checkPermits(permits);
        if (permits > policy.burst()) {
            return RateLimitDecision.denied(policy.permits(), 0, Duration.ZERO);
        }
        long now = ticker.nanoTime();
        return store.apply(key, now, bucket -> decide(bucket, now, permits, true));
    }

    @Override
    public RateLimitDecision peek(String key, long permits) {
        Limiters.checkPermits(permits);
        if (permits > policy.burst()) {
            return RateLimitDecision.denied(policy.permits(), 0, Duration.ZERO);
        }
        long now = ticker.nanoTime();
        return store.apply(key, now, bucket -> decide(bucket, now, permits, false));
    }

    Transition<Bucket, RateLimitDecision> decide(Bucket bucket, long now, long permits, boolean consume) {
        Bucket drained = drain(bucket, now);
        double capacity = policy.burst();

        if (drained.levelNanos() + permits <= capacity) {
            Bucket next = consume ? new Bucket(drained.levelNanos() + permits, now) : drained;
            long headroom = (long) Math.floor(capacity - next.levelNanos());
            return Transition.of(next, RateLimitDecision.allowed(policy.permits(), Math.max(0, headroom)));
        }

        double overflow = drained.levelNanos() + permits - capacity;
        long waitNanos = (long) Math.ceil(overflow / leakPerNano);
        long headroom = (long) Math.floor(capacity - drained.levelNanos());
        return Transition.of(
                drained,
                RateLimitDecision.denied(policy.permits(), Math.max(0, headroom), Duration.ofNanos(waitNanos)));
    }

    private Bucket drain(Bucket bucket, long now) {
        long elapsed = now - bucket.lastLeakNanos();
        if (elapsed <= 0) {
            return bucket;
        }
        double level = Math.max(0.0, bucket.levelNanos() - elapsed * leakPerNano);
        return new Bucket(level, now);
    }

    @Override
    public RateLimitPolicy policy() {
        return policy;
    }

    /**
     * @param levelNanos how full the bucket was at {@code lastLeakNanos}, in permits
     * @param lastLeakNanos tick at which {@code levelNanos} was computed
     */
    public record Bucket(double levelNanos, long lastLeakNanos) {}
}
