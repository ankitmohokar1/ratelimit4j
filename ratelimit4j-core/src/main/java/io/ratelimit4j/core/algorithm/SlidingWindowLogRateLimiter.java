package io.ratelimit4j.core.algorithm;

import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.store.KeyedStateStore;
import io.ratelimit4j.core.store.Transition;
import io.ratelimit4j.core.time.Ticker;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/**
 * Sliding window log: keeps the timestamp of every permit and counts those inside the trailing window.
 *
 * <p>The only exact algorithm here. "At most N in any window of length W" holds precisely, with no
 * boundary spike and no statistical approximation — because the limiter simply remembers when
 * everything happened.
 *
 * <h2>The cost</h2>
 *
 * O(N) memory per key, where N is the permit budget, and O(N) work to copy the log on each request.
 * That makes it unsuitable as a general-purpose limiter: a 10,000/hour policy over a million keys is
 * 80 GB of timestamps.
 *
 * <p>It earns its place on <strong>small, high-value limits</strong> — five failed logins per hour,
 * three password resets per day, one account creation per IP per minute. There, N is single digits,
 * the key space is small, and being exactly right matters more than being cheap. Approximating a
 * login limiter is how "five attempts per hour" quietly becomes nine.
 *
 * <h2>Implementation</h2>
 *
 * The log is an immutable, sorted {@code long[]}, copied on every write. That sounds wasteful, and for
 * large N it is — but it is what lets this share the lock-free CAS store with every other algorithm:
 * a mutable deque would need its own lock, and the copy of a ten-element array costs less than
 * acquiring one. Entries are appended in tick order, so the array is sorted by construction and
 * expiry is a prefix scan rather than a search.
 *
 * <p>{@code burst} is ignored; the log's own size limit is the burst.
 */
public final class SlidingWindowLogRateLimiter implements RateLimiter {

    private static final long[] EMPTY = new long[0];

    private final RateLimitPolicy policy;
    private final Ticker ticker;
    private final KeyedStateStore<Log> store;
    private final long windowNanos;

    public SlidingWindowLogRateLimiter(RateLimitPolicy policy, Ticker ticker) {
        this(policy, ticker, Limiters.defaultStore(policy, ticker, now -> new Log(EMPTY)));
    }

    public SlidingWindowLogRateLimiter(RateLimitPolicy policy, Ticker ticker, KeyedStateStore<Log> store) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        this.store = Objects.requireNonNull(store, "store");
        this.windowNanos = policy.window().toNanos();
        if (policy.permits() > 100_000) {
            throw new IllegalArgumentException(
                    "sliding window log stores one timestamp per permit; a limit of "
                            + policy.permits()
                            + " would allocate "
                            + (policy.permits() * 8 / 1024)
                            + " KiB per key. Use SlidingWindowCounterRateLimiter for limits this large.");
        }
    }

    @Override
    public RateLimitDecision tryAcquire(String key, long permits) {
        Limiters.checkPermits(permits);
        if (permits > policy.permits()) {
            return RateLimitDecision.denied(policy.permits(), 0, Duration.ZERO);
        }
        long now = ticker.nanoTime();
        return store.apply(key, now, log -> decide(log, now, permits, true));
    }

    @Override
    public RateLimitDecision peek(String key, long permits) {
        Limiters.checkPermits(permits);
        if (permits > policy.permits()) {
            return RateLimitDecision.denied(policy.permits(), 0, Duration.ZERO);
        }
        long now = ticker.nanoTime();
        return store.apply(key, now, log -> decide(log, now, permits, false));
    }

    Transition<Log, RateLimitDecision> decide(Log log, long now, long permits, boolean consume) {
        long[] live = prune(log.timestamps(), now);
        long used = live.length;

        if (used + permits <= policy.permits()) {
            if (!consume) {
                return Transition.of(
                        new Log(live), RateLimitDecision.allowed(policy.permits(), policy.permits() - used));
            }
            long[] next = Arrays.copyOf(live, (int) (used + permits));
            Arrays.fill(next, (int) used, next.length, now);
            return Transition.of(
                    new Log(next), RateLimitDecision.allowed(policy.permits(), policy.permits() - next.length));
        }

        // The oldest entry that must age out before there is room. Exact, not estimated: the caller is
        // told the precise instant the request would succeed.
        int mustExpire = (int) (used + permits - policy.permits());
        long freeAt = live[mustExpire - 1] + windowNanos;
        return Transition.of(
                new Log(live),
                RateLimitDecision.denied(
                        policy.permits(),
                        Math.max(0, policy.permits() - used),
                        Duration.ofNanos(Math.max(0, freeAt - now))));
    }

    /**
     * Drops entries that have fallen out of the trailing window.
     *
     * <p>A linear prefix scan rather than a binary search: the array is at most a few hundred entries
     * by the constructor's own limit, and at that size a scan wins on cache behaviour. It also returns
     * the input array unchanged when nothing expired, which is the common case on a busy key and skips
     * the copy entirely.
     */
    private long[] prune(long[] timestamps, long now) {
        long cutoff = now - windowNanos;
        int firstLive = 0;
        while (firstLive < timestamps.length && timestamps[firstLive] <= cutoff) {
            firstLive++;
        }
        if (firstLive == 0) {
            return timestamps;
        }
        return Arrays.copyOfRange(timestamps, firstLive, timestamps.length);
    }

    @Override
    public RateLimitPolicy policy() {
        return policy;
    }

    /**
     * @param timestamps ticks at which permits were spent, ascending. Treated as immutable; never
     *     mutated in place after publication.
     */
    public record Log(long[] timestamps) {

        public Log {
            Objects.requireNonNull(timestamps, "timestamps");
        }

        /**
         * Records override equality to compare array references, which is never what a caller means
         * here; both accessors are overridden to compare contents instead.
         */
        @Override
        public boolean equals(Object o) {
            return o instanceof Log other && Arrays.equals(timestamps, other.timestamps);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(timestamps);
        }

        @Override
        public String toString() {
            return "Log" + Arrays.toString(timestamps);
        }
    }
}
