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
 * Sliding window counter: a fixed window that also carries a weighted share of the previous one.
 *
 * <p>This is the algorithm Cloudflare described using at scale, and it is the best
 * accuracy-per-byte trade in this library.
 *
 * <h2>How it works</h2>
 *
 * Two counters are kept: the current window's and the previous window's. The effective usage is the
 * current count plus the previous count scaled by how much of the previous window still falls inside
 * the trailing window:
 *
 * <pre>
 *   elapsed  = now − currentWindowStart
 *   weight   = 1 − elapsed / windowLength
 *   estimate = previousCount × weight + currentCount
 * </pre>
 *
 * <p>A quarter of the way into the current window, three quarters of the previous window is still
 * within the last {@code windowLength}, so three quarters of its count is charged. This slides
 * smoothly and removes the fixed window's boundary spike — the previous window's traffic keeps
 * counting against the client as it ages out, instead of vanishing at the boundary.
 *
 * <h2>What it approximates away</h2>
 *
 * The estimate assumes the previous window's requests were spread evenly across it. They may not have
 * been. If a client sent all of its previous-window traffic in that window's first instant, that
 * traffic is genuinely outside the trailing window sooner than the weight believes, and the client is
 * over-charged; if it sent it all at the very end, it is under-charged. Cloudflare's published figure
 * for the resulting error on production traffic is well under 1%, which for a defensive control is far
 * cheaper than the exactness {@link SlidingWindowLogRateLimiter} charges O(n) memory for.
 *
 * <p>{@code burst} is ignored, as with any window-based counter.
 */
public final class SlidingWindowCounterRateLimiter implements RateLimiter {

    private final RateLimitPolicy policy;
    private final Ticker ticker;
    private final KeyedStateStore<Windows> store;
    private final long windowNanos;

    public SlidingWindowCounterRateLimiter(RateLimitPolicy policy, Ticker ticker) {
        this(policy, ticker, Limiters.defaultStore(policy, ticker, now -> new Windows(now, 0, 0)));
    }

    public SlidingWindowCounterRateLimiter(
            RateLimitPolicy policy, Ticker ticker, KeyedStateStore<Windows> store) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        this.store = Objects.requireNonNull(store, "store");
        this.windowNanos = policy.window().toNanos();
    }

    @Override
    public RateLimitDecision tryAcquire(String key, long permits) {
        Limiters.checkPermits(permits);
        if (permits > policy.permits()) {
            return RateLimitDecision.denied(policy.permits(), 0, Duration.ZERO);
        }
        long now = ticker.nanoTime();
        return store.apply(key, now, w -> decide(w, now, permits, true));
    }

    @Override
    public RateLimitDecision peek(String key, long permits) {
        Limiters.checkPermits(permits);
        if (permits > policy.permits()) {
            return RateLimitDecision.denied(policy.permits(), 0, Duration.ZERO);
        }
        long now = ticker.nanoTime();
        return store.apply(key, now, w -> decide(w, now, permits, false));
    }

    Transition<Windows, RateLimitDecision> decide(Windows state, long now, long permits, boolean consume) {
        Windows current = roll(state, now);
        double estimate = estimate(current, now);

        if (estimate + permits <= policy.permits()) {
            Windows next = consume
                    ? new Windows(current.startNanos(), current.previousCount(), current.currentCount() + permits)
                    : current;
            long remaining = (long) Math.floor(policy.permits() - estimate(next, now));
            return Transition.of(next, RateLimitDecision.allowed(policy.permits(), Math.max(0, remaining)));
        }

        return Transition.of(
                current,
                RateLimitDecision.denied(
                        policy.permits(),
                        Math.max(0, (long) Math.floor(policy.permits() - estimate)),
                        retryAfter(current, now, permits, estimate)));
    }

    private double estimate(Windows w, long now) {
        long elapsed = now - w.startNanos();
        double weight = 1.0 - (double) elapsed / windowNanos;
        if (weight < 0) {
            weight = 0;
        }
        return w.previousCount() * weight + w.currentCount();
    }

    /**
     * How long until the estimate decays far enough to admit {@code permits}.
     *
     * <p>Solved in closed form rather than by stepping. With no new traffic the estimate is a
     * continuous, piecewise-linear, decreasing function of time, with a kink at each window boundary,
     * so the instant it crosses the target is a root that can be read straight off whichever segment
     * contains it.
     *
     * <p>The boundary is the part that is easy to get wrong. It is tempting to answer "wait until the
     * window turns over", but the estimate does not drop at the turn — it is continuous across it. At
     * the boundary the current window's count simply becomes the previous window's, and starts
     * decaying in its place. A client told to wait exactly one window therefore comes back to find the
     * same estimate it left, and is refused again. So two segments are considered:
     *
     * <ol>
     *   <li>the rest of the current window, over which the estimate falls from its present value to
     *       the current window's own count, at a rate set by the previous window's count;
     *   <li>the whole of the next window, over which what is now the current count decays away in
     *       turn.
     * </ol>
     */
    private Duration retryAfter(Windows w, long now, long permits, double estimate) {
        // The estimate must fall to at most this for the request to fit.
        double target = policy.permits() - permits;
        long toBoundary = Math.max(0, w.startNanos() + windowNanos - now);

        // Segment 1 — reachable only while the previous window still contributes something to decay.
        if (target >= w.currentCount() && w.previousCount() > 0) {
            double decayPerNano = (double) w.previousCount() / windowNanos;
            long waitNanos = (long) Math.ceil((estimate - target) / decayPerNano);
            return Duration.ofNanos(Math.max(0, Math.min(waitNanos, toBoundary)));
        }

        // Segment 2 — the current window's own count is what stands in the way.
        if (w.currentCount() <= 0) {
            return Duration.ofNanos(toBoundary);
        }
        if (target <= 0) {
            // Nothing short of a full drain will do.
            return Duration.ofNanos(toBoundary + windowNanos);
        }
        long intoNext = (long) Math.ceil(windowNanos * (1.0 - target / w.currentCount()));
        return Duration.ofNanos(toBoundary + Math.max(0, Math.min(intoNext, windowNanos)));
    }

    /**
     * Advances the window pair to cover {@code now}.
     *
     * <p>Exactly one window elapsed: the current count becomes the previous. Two or more: everything
     * older is fully outside the trailing window, so both counters reset.
     */
    private Windows roll(Windows w, long now) {
        long elapsed = now - w.startNanos();
        if (elapsed < windowNanos) {
            return w;
        }
        if (elapsed < 2 * windowNanos) {
            return new Windows(w.startNanos() + windowNanos, w.currentCount(), 0);
        }
        long windowsPassed = elapsed / windowNanos;
        return new Windows(w.startNanos() + windowsPassed * windowNanos, 0, 0);
    }

    @Override
    public RateLimitPolicy policy() {
        return policy;
    }

    /**
     * @param startNanos tick at which the current window opened
     * @param previousCount permits spent in the window before this one
     * @param currentCount permits spent in the current window
     */
    public record Windows(long startNanos, long previousCount, long currentCount) {}
}
