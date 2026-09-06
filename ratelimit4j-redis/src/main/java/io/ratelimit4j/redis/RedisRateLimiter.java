package io.ratelimit4j.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.algorithm.Algorithm;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * A rate limiter whose state lives in Redis, so a fleet of nodes enforces one shared budget.
 *
 * <h2>Why this exists</h2>
 *
 * An in-process limiter on N nodes enforces N times the intended limit, and the effective limit
 * changes whenever the fleet scales or a node restarts. Any limit that is a real contract — a
 * customer's plan, a downstream service's capacity — has to be shared.
 *
 * <h2>Atomicity</h2>
 *
 * Each decision is one Lua script invocation. Redis executes scripts on a single thread with nothing
 * interleaved, so read-modify-write is atomic across the whole fleet with no lock, no WATCH/MULTI
 * retry loop, and one round trip. This is the same {@code (state, now) -> (nextState, decision)}
 * transition the in-process algorithms use; only the concurrency mechanism differs.
 *
 * <h2>Time</h2>
 *
 * Scripts read Redis's clock via {@code TIME} rather than accepting a caller timestamp. Redis is then
 * the single authoritative clock for the fleet, and the limit no longer depends on how well the
 * nodes' clocks agree — with client timestamps, the effective limit is a function of the worst clock
 * skew among them, which is not a property anyone wants to operate.
 *
 * <h2>Availability</h2>
 *
 * Every decision requires a Redis round trip, so Redis is on the critical path of every request. That
 * is a real cost, in latency and in one more thing that can fail. {@link ResilientRateLimiter} is the
 * answer to the failure half: wrap this so an unreachable Redis degrades to a local decision rather
 * than an error. Do not deploy this class bare in a path that has to stay up.
 *
 * <p>Not every algorithm is available here. Sliding window log would need the whole timestamp set
 * shipped or manipulated per request, and the exactness it buys does not survive being spread over a
 * network anyway; {@link #supports} reports what is implemented.
 */
public final class RedisRateLimiter implements RateLimiter {

    private static final LuaScript GCRA = LuaScript.load("gcra.lua");
    private static final LuaScript TOKEN_BUCKET = LuaScript.load("token_bucket.lua");
    private static final LuaScript SLIDING_WINDOW_COUNTER = LuaScript.load("sliding_window_counter.lua");

    private final StatefulRedisConnection<String, String> connection;
    private final RateLimitPolicy policy;
    private final Algorithm algorithm;
    private final String keyPrefix;
    private final LuaScript script;
    private final String[] baseArgs;
    private final long ttlMillis;

    public RedisRateLimiter(
            StatefulRedisConnection<String, String> connection,
            RateLimitPolicy policy,
            Algorithm algorithm,
            String keyPrefix) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.algorithm = Objects.requireNonNull(algorithm, "algorithm");
        this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix");

        if (!supports(algorithm)) {
            throw new IllegalArgumentException(
                    algorithm + " has no Redis implementation; supported: GCRA, TOKEN_BUCKET, SLIDING_WINDOW_COUNTER");
        }

        // Twice the window, floored at a minute — the same reasoning as the in-process store's
        // retention. Redis reclaims idle keys by TTL, so nothing here has to sweep.
        this.ttlMillis = Math.max(policy.window().toMillis() * 2, Duration.ofMinutes(1).toMillis());

        long intervalMicros = Math.max(1, policy.window().toNanos() / 1_000 / policy.permits());
        this.script = scriptFor(algorithm);
        this.baseArgs = switch (algorithm) {
            case GCRA -> new String[] {
                Long.toString(intervalMicros), Long.toString(intervalMicros * policy.burst())
            };
            case TOKEN_BUCKET -> new String[] {
                Long.toString(policy.burst()),
                // Permits per microsecond, as a decimal Lua can parse.
                Double.toString(policy.permitsPerNano() * 1_000)
            };
            case SLIDING_WINDOW_COUNTER -> new String[] {
                Long.toString(policy.window().toNanos() / 1_000), Long.toString(policy.permits())
            };
            default -> throw new IllegalStateException("unreachable: " + algorithm);
        };
    }

    /**
     * @return whether {@code algorithm} has a Redis implementation
     */
    public static boolean supports(Algorithm algorithm) {
        return algorithm == Algorithm.GCRA
                || algorithm == Algorithm.TOKEN_BUCKET
                || algorithm == Algorithm.SLIDING_WINDOW_COUNTER;
    }

    private static LuaScript scriptFor(Algorithm algorithm) {
        return switch (algorithm) {
            case GCRA -> GCRA;
            case TOKEN_BUCKET -> TOKEN_BUCKET;
            case SLIDING_WINDOW_COUNTER -> SLIDING_WINDOW_COUNTER;
            default -> throw new IllegalArgumentException(algorithm + " has no Redis implementation");
        };
    }

    @Override
    public RateLimitDecision tryAcquire(String key, long permits) {
        return evaluate(key, permits, true);
    }

    @Override
    public RateLimitDecision peek(String key, long permits) {
        return evaluate(key, permits, false);
    }

    private RateLimitDecision evaluate(String key, long permits, boolean consume) {
        Objects.requireNonNull(key, "key");
        if (permits <= 0) {
            throw new IllegalArgumentException("permits must be positive, got " + permits);
        }
        long ceiling = algorithm == Algorithm.SLIDING_WINDOW_COUNTER ? policy.permits() : policy.burst();
        if (permits > ceiling) {
            // Unsatisfiable however long the caller waits; say so rather than advertising a delay.
            return RateLimitDecision.denied(policy.permits(), 0, Duration.ZERO);
        }

        String[] args = new String[baseArgs.length + 3];
        System.arraycopy(baseArgs, 0, args, 0, baseArgs.length);
        args[baseArgs.length] = Long.toString(permits);
        args[baseArgs.length + 1] = Long.toString(ttlMillis);
        args[baseArgs.length + 2] = consume ? "1" : "0";

        List<Long> reply = script.eval(connection.sync(), new String[] {keyPrefix + key}, args);
        boolean allowed = reply.get(0) == 1L;
        long remaining = Math.max(0, reply.get(1));
        if (allowed) {
            return RateLimitDecision.allowed(policy.permits(), remaining);
        }
        return RateLimitDecision.denied(
                policy.permits(), remaining, Duration.ofNanos(reply.get(2) * 1_000));
    }

    @Override
    public RateLimitPolicy policy() {
        return policy;
    }
}
