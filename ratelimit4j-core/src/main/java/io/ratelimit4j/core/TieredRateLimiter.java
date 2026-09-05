package io.ratelimit4j.core;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Enforces several policies at once — "10 per second <em>and</em> 1,000 per hour" — granting a request
 * only if every tier allows it.
 *
 * <p>Real quotas are almost always tiered. A per-second limit protects the service from a stampede; a
 * per-hour or per-day limit stops a client consuming a month of quota in an afternoon. Neither alone
 * does both jobs.
 *
 * <h2>Why permits are not simply charged to every tier</h2>
 *
 * The obvious implementation calls {@code tryAcquire} on each tier in turn and fails if any refuses.
 * It is wrong: if the per-second tier allows and the per-hour tier refuses, the request was rejected
 * but the per-second tier has already spent a permit on it. A client sitting against its hourly limit
 * silently burns its per-second budget on requests that never ran, and the two tiers drift apart.
 *
 * <p>So this class runs in two phases: {@link RateLimiter#peek} every tier, and only if all of them
 * would allow does it commit by calling {@code tryAcquire} on each.
 *
 * <h2>What that still does not guarantee</h2>
 *
 * Peek-then-commit is not atomic across tiers. Between the peek and the commit another thread can
 * exhaust a tier, and this request will then commit into an over-subscribed one — so a tier can be
 * pushed slightly past its limit under concurrency, bounded by the number of threads racing.
 *
 * <p>Closing that gap needs a lock covering all tiers, and that lock would sit on the hot path of
 * every request, serialising the limiter for exactly the traffic it exists to survive. For a
 * defensive control the trade is clear: a bounded overshoot under contention costs far less than
 * making the limiter itself the bottleneck. Where the overshoot is unacceptable — a hard billing
 * ceiling, say — enforce that tier alone, where a single limiter's own atomicity is the guarantee.
 *
 * <p>Tiers are checked in the order given. Put the cheapest and most selective first: a request
 * rejected by tier one never touches tier two.
 */
public final class TieredRateLimiter implements RateLimiter {

    private final List<RateLimiter> tiers;

    public TieredRateLimiter(List<RateLimiter> tiers) {
        Objects.requireNonNull(tiers, "tiers");
        if (tiers.isEmpty()) {
            throw new IllegalArgumentException("a tiered limiter needs at least one tier");
        }
        this.tiers = List.copyOf(tiers);
    }

    public static TieredRateLimiter of(RateLimiter... tiers) {
        return new TieredRateLimiter(List.of(tiers));
    }

    @Override
    public RateLimitDecision tryAcquire(String key, long permits) {
        RateLimitDecision blocking = firstRefusal(key, permits);
        if (blocking != null) {
            return blocking;
        }
        // Every tier said yes; charge them all. See the class Javadoc for the race this leaves open.
        RateLimitDecision tightest = null;
        for (RateLimiter tier : tiers) {
            RateLimitDecision decision = tier.tryAcquire(key, permits);
            if (decision.denied()) {
                // Lost a race since the peek. Report the refusal; the permits already charged to
                // earlier tiers are conceded rather than rolled back, because a rollback would itself
                // race and could hand back permits another thread has since spent.
                return decision;
            }
            if (tightest == null || decision.remaining() < tightest.remaining()) {
                tightest = decision;
            }
        }
        return tightest;
    }

    @Override
    public RateLimitDecision peek(String key, long permits) {
        RateLimitDecision blocking = firstRefusal(key, permits);
        return blocking != null ? blocking : tightestOf(key, permits);
    }

    /**
     * @return the decision of the first tier that would refuse, or {@code null} if all would allow
     */
    private RateLimitDecision firstRefusal(String key, long permits) {
        for (RateLimiter tier : tiers) {
            RateLimitDecision decision = tier.peek(key, permits);
            if (decision.denied()) {
                return decision;
            }
        }
        return null;
    }

    /**
     * The tier with the least headroom — the one that will refuse first, and so the honest number to
     * report to the client in {@code RateLimit-Remaining}.
     */
    private RateLimitDecision tightestOf(String key, long permits) {
        RateLimitDecision tightest = null;
        for (RateLimiter tier : tiers) {
            RateLimitDecision decision = tier.peek(key, permits);
            if (tightest == null || decision.remaining() < tightest.remaining()) {
                tightest = decision;
            }
        }
        return tightest;
    }

    /**
     * @return the policy of the most restrictive tier by sustained rate. A tiered limiter has no single
     *     policy; this reports the one a client will hit first in steady state.
     */
    @Override
    public RateLimitPolicy policy() {
        RateLimitPolicy strictest = tiers.get(0).policy();
        for (RateLimiter tier : tiers) {
            if (tier.policy().permitsPerNano() < strictest.permitsPerNano()) {
                strictest = tier.policy();
            }
        }
        return strictest;
    }

    /**
     * @return the tiers, in evaluation order
     */
    public List<RateLimiter> tiers() {
        return tiers;
    }
}
