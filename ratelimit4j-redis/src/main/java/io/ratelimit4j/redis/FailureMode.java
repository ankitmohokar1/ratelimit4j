package io.ratelimit4j.redis;

/**
 * What a limiter should do when its distributed backend cannot be reached.
 *
 * <p>There is no safe default here, only a choice about which failure you would rather have, and it
 * depends entirely on what the limit is protecting.
 */
public enum FailureMode {

    /**
     * Allow the request.
     *
     * <p>Correct when the limiter protects against <em>excess</em> rather than <em>abuse</em>: a
     * fairness quota, a politeness limit, a courtesy cap on an internal API. A Redis outage should not
     * become an outage of the service in front of it.
     *
     * <p>The risk is that an attacker who can take Redis down has also taken the limiter down. If the
     * limit is the only thing standing between an attacker and the thing behind it, this is the wrong
     * choice.
     */
    FAIL_OPEN,

    /**
     * Reject the request.
     *
     * <p>Correct when exceeding the limit is worse than being unavailable: a hard billing ceiling, a
     * downstream service that falls over above a known rate, a credential-stuffing defence.
     *
     * <p>The risk is obvious and severe — a Redis outage becomes a total outage of everything behind
     * the limiter. Choose this deliberately, and only when it has been said out loud that a Redis
     * failure is meant to take the service with it.
     */
    FAIL_CLOSED,

    /**
     * Fall back to an in-process limiter enforcing the same policy locally.
     *
     * <p>Almost always the right answer, and the default for {@link ResilientRateLimiter}. The
     * effective limit becomes N times the intended one across N nodes, which is a bounded, understood
     * degradation — not the unbounded exposure of failing open, and not the outage of failing closed.
     * A limiter that is approximately right beats one that is exactly absent.
     */
    LOCAL_FALLBACK
}
