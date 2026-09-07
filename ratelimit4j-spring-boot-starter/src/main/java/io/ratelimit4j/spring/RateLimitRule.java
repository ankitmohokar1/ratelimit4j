package io.ratelimit4j.spring;

import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import java.util.Objects;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;

/**
 * A path pattern paired with the limiter that enforces it.
 *
 * @param pattern an Ant-style path pattern, e.g. {@code /api/**}
 * @param limiter the limiter applied to matching requests
 */
public record RateLimitRule(String pattern, RateLimiter limiter) {

    private static final PathMatcher MATCHER = new AntPathMatcher();

    public RateLimitRule {
        Objects.requireNonNull(pattern, "pattern");
        Objects.requireNonNull(limiter, "limiter");
    }

    public boolean matches(String path) {
        return MATCHER.match(pattern, path);
    }

    public RateLimitPolicy policy() {
        return limiter.policy();
    }
}
