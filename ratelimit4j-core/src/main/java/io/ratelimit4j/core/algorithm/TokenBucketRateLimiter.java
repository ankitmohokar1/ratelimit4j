package io.ratelimit4j.core.algorithm;

import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.store.ConcurrentKeyedStateStore;
import io.ratelimit4j.core.store.KeyedStateStore;
import io.ratelimit4j.core.store.Transition;
import io.ratelimit4j.core.time.Ticker;
import java.time.Duration;
import java.util.Objects;

/**
 * Token bucket: a bucket of {@code burst} tokens refilled continuously at the sustained rate.
 *
 * <p>A request spending {@code n} permits succeeds if at least {@code n} tokens are present, and
 * removes them. Tokens accrue at {@code permits/window} and stop at {@code burst}.
 *
 * <p><strong>Why this one is the default.</strong> It allows a genuine burst — a client that has been
 * idle can spend its accumulated allowance immediately — while still bounding the long-run rate. That
 * matches how real clients behave (bursty, not smooth) without letting a client sustain the burst
 * rate forever.
 *
 * <p><strong>Refill is lazy.</strong> Nothing ticks in the background. Each request computes how many
 * tokens have accrued since the previous one, which makes the algorithm O(1) in both time and memory
 * per key, regardless of how long the key sat idle.
 *
 * <table border="1">
 *   <caption>Characteristics</caption>
 *   <tr><td>Time per request</td><td>O(1)</td></tr>
 *   <tr><td>Memory per key</td><td>16 bytes of state (one double, one long)</td></tr>
 *   <tr><td>Burst</td><td>Yes, up to {@code burst}</td></tr>
 *   <tr><td>Boundary spike</td><td>No</td></tr>
 * </table>
 *
 * <p>The one wart: tokens are a {@code double}, so a bucket refilled in very small increments
 * accumulates floating-point error. At the rates this is used for the error stays many orders of
 * magnitude below one token, and {@link GcraRateLimiter} offers an integer-only alternative for
 * callers who would rather not reason about it at all.
 */
public final class TokenBucketRateLimiter implements RateLimiter {

    private final RateLimitPolicy policy;
    private final Ticker ticker;
    private final KeyedStateStore<Bucket> store;
    private final double tokensPerNano;

    public TokenBucketRateLimiter(RateLimitPolicy policy, Ticker ticker) {
        this(policy, ticker, Limiters.defaultStore(policy, ticker, now -> new Bucket(policy.burst(), now)));
    }

    public TokenBucketRateLimiter(RateLimitPolicy policy, Ticker ticker, KeyedStateStore<Bucket> store) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        this.store = Objects.requireNonNull(store, "store");
        this.tokensPerNano = policy.permitsPerNano();
    }

    @Override
    public RateLimitDecision tryAcquire(String key, long permits) {
        Limiters.checkPermits(permits);
        if (permits > policy.burst()) {
            // Unsatisfiable at any point in time: the bucket never holds this many tokens. Reject now
            // rather than reporting a retryAfter the caller would wait out for nothing.
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
        // Refill still happens on a peek: it is a pure function of elapsed time, not of the request,
        // so writing it back keeps the stored state canonical without granting anything.
        return store.apply(key, now, bucket -> decide(bucket, now, permits, false));
    }

    /**
     * The whole algorithm, as a pure function. Package-private so tests can drive it directly with a
     * hand-built {@link Bucket} rather than going through a store.
     */
    Transition<Bucket, RateLimitDecision> decide(Bucket bucket, long now, long permits, boolean consume) {
        Bucket refilled = refill(bucket, now);
        if (refilled.tokens() >= permits) {
            Bucket next = consume ? new Bucket(refilled.tokens() - permits, now) : refilled;
            long remaining = (long) Math.floor(next.tokens());
            return Transition.of(next, RateLimitDecision.allowed(policy.permits(), remaining));
        }
        double shortfall = permits - refilled.tokens();
        long waitNanos = (long) Math.ceil(shortfall / tokensPerNano);
        return Transition.of(
                refilled,
                RateLimitDecision.denied(
                        policy.permits(), (long) Math.floor(refilled.tokens()), Duration.ofNanos(waitNanos)));
    }

    private Bucket refill(Bucket bucket, long now) {
        long elapsed = now - bucket.lastRefillNanos();
        if (elapsed <= 0) {
            // Either a genuinely simultaneous request or a ticker that failed to advance. Either way,
            // adding a negative refill would silently hand out tokens the client has not earned.
            return bucket;
        }
        double replenished = Math.min(policy.burst(), bucket.tokens() + elapsed * tokensPerNano);
        return new Bucket(replenished, now);
    }

    @Override
    public RateLimitPolicy policy() {
        return policy;
    }

    /**
     * @param tokens tokens available as of {@code lastRefillNanos}
     * @param lastRefillNanos tick at which {@code tokens} was computed
     */
    public record Bucket(double tokens, long lastRefillNanos) {}
}
