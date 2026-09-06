package io.ratelimit4j.redis;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Lua script loaded from the classpath and executed against Redis by SHA.
 *
 * <p>Sending the script body on every request would waste bandwidth proportional to traffic, so the
 * body is uploaded once and subsequently invoked by its SHA-1 digest via {@code EVALSHA}.
 *
 * <p>The complication is that Redis's script cache is not durable: it is emptied by {@code SCRIPT
 * FLUSH}, by a restart, and — the case that actually bites in production — by a failover onto a
 * replica that was never sent the script. Any of those turns the next {@code EVALSHA} into a
 * {@code NOSCRIPT} error. This class handles that by falling back to a full {@code EVAL}, which both
 * satisfies the current request and re-populates the cache for the next one. Without that fallback
 * the limiter fails every request after a failover, which for a fail-closed deployment means an
 * outage.
 */
final class LuaScript {

    private static final Logger log = LoggerFactory.getLogger(LuaScript.class);

    private final String body;
    private final AtomicReference<String> digest = new AtomicReference<>();

    private LuaScript(String body) {
        this.body = body;
    }

    /**
     * Reads a script from the classpath, relative to this package.
     *
     * @throws IllegalStateException if the resource is missing, which can only mean a broken build
     */
    static LuaScript load(String resourceName) {
        try (InputStream in = LuaScript.class.getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalStateException("Lua script not found on the classpath: " + resourceName);
            }
            return new LuaScript(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IllegalStateException("could not read Lua script " + resourceName, e);
        }
    }

    /**
     * Runs the script, uploading it first if Redis does not have it.
     *
     * @return the script's reply, as a Lua array of integers
     */
    @SuppressWarnings("unchecked")
    java.util.List<Long> eval(RedisCommands<String, String> commands, String[] keys, String[] args) {
        Objects.requireNonNull(commands, "commands");
        String sha = digest.get();
        if (sha != null) {
            try {
                return (java.util.List<Long>) commands.evalsha(sha, ScriptOutputType.MULTI, keys, args);
            } catch (RedisNoScriptException e) {
                // Redis lost the script — a restart, a SCRIPT FLUSH, or a failover onto a replica that
                // never had it. Drop the stale digest and fall through to a full EVAL.
                log.debug("Redis has no cached script for digest {}; re-uploading", sha);
                digest.compareAndSet(sha, null);
            }
        }
        java.util.List<Long> result =
                (java.util.List<Long>) commands.eval(body, ScriptOutputType.MULTI, keys, args);
        // EVAL populates the cache as a side effect, so the digest is now valid for subsequent calls.
        digest.set(sha1(body));
        return result;
    }

    /**
     * The SHA-1 of the script body, which is exactly how Redis keys its own script cache.
     *
     * <p>Computed locally rather than taken from {@code SCRIPT LOAD} so that no extra round trip is
     * needed. SHA-1 is used here because Redis's protocol specifies it, not as a security choice.
     */
    private static String sha1(String script) {
        try {
            byte[] hash = java.security.MessageDigest.getInstance("SHA-1")
                    .digest(script.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 is required by the Redis scripting protocol", e);
        }
    }
}
