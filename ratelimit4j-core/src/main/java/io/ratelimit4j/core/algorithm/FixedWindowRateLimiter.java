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
 * Fixed window counter: a counter per key, reset every window.
 *
 * <p>The simplest correct thing that can be built, and included here mostly so the alternatives have
 * something to be compared against.
 *
 * <h2>The boundary spike</h2>
 *
 * Fixed windows admit up to <em>twice</em> the intended rate across a window boundary. With a limit of
 * 100/minute, a client that sends 100 requests at 11:59:59 and another 100 at 12:00:01 has sent 200
 * requests in two seconds, and every one of them was inside policy. Whether that matters depends
 * entirely on why the limit exists: it is fine for billing quotas, and not fine for protecting a
 * downstream service whose actual constraint is instantaneous concurrency.
 *
 * <p>{@link SlidingWindowCounterRateLimiter} fixes this for one extra counter per key, which is
 * usually the better trade. Reach for a fixed window when the window boundary is externally
 * meaningful — a calendar-month quota, say, where resetting on the boundary is the point.
 *
 * <p>{@code burst} is ignored: a fixed window has no separate burst notion, since the entire budget is
 * spendable in an instant by construction.
 */
public final class FixedWindowRateLimiter implements RateLimiter {

    private final RateLimitPolicy policy;
    private final Ticker ticker;
    private final KeyedStateStore<Window> store;
    private final long windowNanos;

    public FixedWindowRateLimiter(RateLimitPolicy policy, Ticker ticker) {
        this(policy, ticker, Limiters.defaultStore(policy, ticker, now -> new Window(now, 0)));
    }

    public FixedWindowRateLimiter(RateLimitPolicy policy, Ticker ticker, KeyedStateStore<Window> store) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        this.store = Objects.requireNonNull(store, "store");
        this.windowNanos = policy.window().toNanos();
    }

    @Override
    public RateLimitDecision tryAcquire(String key, long permits) {
        Limiters.checkPermits(permits);
        long now = ticker.nanoTime();
        return store.apply(key, now, window -> decide(window, now, permits, true));
    }

    @Override
    public RateLimitDecision peek(String key, long permits) {
        Limiters.checkPermits(permits);
        long now = ticker.nanoTime();
        return store.apply(key, now, window -> decide(window, now, permits, false));
    }

    Transition<Window, RateLimitDecision> decide(Window window, long now, long permits, boolean consume) {
        Window current = roll(window, now);
        long used = current.count();
        if (used + permits <= policy.permits()) {
            Window next = consume ? new Window(current.startNanos(), used + permits) : current;
            return Transition.of(
                    next, RateLimitDecision.allowed(policy.permits(), policy.permits() - next.count()));
        }
        long resetIn = current.startNanos() + windowNanos - now;
        return Transition.of(
                current,
                RateLimitDecision.denied(
                        policy.permits(),
                        Math.max(0, policy.permits() - used),
                        Duration.ofNanos(Math.max(0, resetIn))));
    }

    /**
     * Advances to the window containing {@code now}, resetting the count if the old one has elapsed.
     *
     * <p>The new window is aligned to a multiple of the window length from the original start rather
     * than to {@code now}. Anchoring to {@code now} would let a client that arrives late in every
     * window drag the boundary forward indefinitely, stretching an N-per-window limit into something
     * slower and harder to reason about.
     */
    private Window roll(Window window, long now) {
        long elapsed = now - window.startNanos();
        if (elapsed < windowNanos) {
            return window;
        }
        long windowsPassed = elapsed / windowNanos;
        return new Window(window.startNanos() + windowsPassed * windowNanos, 0);
    }

    @Override
    public RateLimitPolicy policy() {
        return policy;
    }

    /**
     * @param startNanos tick at which the current window opened
     * @param count permits spent so far in the current window
     */
    public record Window(long startNanos, long count) {}
}
