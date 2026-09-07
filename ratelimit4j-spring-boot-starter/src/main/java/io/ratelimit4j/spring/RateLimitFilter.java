package io.ratelimit4j.spring;

import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimiter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Applies the configured rules to inbound requests, refusing those over their limit with 429.
 *
 * <h2>Where this sits</h2>
 *
 * The filter is ordered early — ahead of authentication — so that refused traffic costs as little as
 * possible. That is the entire point of a rate limiter: the work it saves is the work it declines to
 * start. Running it after an expensive filter chain would mean paying for every request an attacker
 * sends, which is the cost the limiter exists to avoid.
 *
 * <p>The trade-off is that limiting cannot key on the authenticated principal at this position. When
 * {@code PRINCIPAL} keying is configured the filter must run after authentication instead, and the
 * auto-configuration orders it accordingly.
 *
 * <h2>Response headers</h2>
 *
 * Successful and refused responses both carry {@code RateLimit-Limit}, {@code RateLimit-Remaining}
 * and {@code RateLimit-Reset}, following the IETF draft. Refusals additionally carry the standard
 * {@code Retry-After}. Telling a client its remaining quota on every response is what lets a
 * well-behaved client pace itself rather than discovering the limit by hitting it.
 */
public class RateLimitFilter extends OncePerRequestFilter {

    /**
     * Ahead of Spring Security's default filter chain order, so refused traffic never reaches it.
     */
    public static final int DEFAULT_ORDER = -200;

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

    private final List<RateLimitRule> rules;
    private final RateLimitKeyResolver keyResolver;
    private final boolean emitHeaders;

    public RateLimitFilter(List<RateLimitRule> rules, RateLimitKeyResolver keyResolver, boolean emitHeaders) {
        this.rules = List.copyOf(Objects.requireNonNull(rules, "rules"));
        this.keyResolver = Objects.requireNonNull(keyResolver, "keyResolver");
        this.emitHeaders = emitHeaders;
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {

        RateLimitRule rule = firstMatching(request.getRequestURI());
        if (rule == null) {
            chain.doFilter(request, response);
            return;
        }

        String key = keyResolver.resolve(request);
        RateLimiter limiter = rule.limiter();
        RateLimitDecision decision = limiter.tryAcquire(key, 1);

        if (emitHeaders) {
            writeRateLimitHeaders(response, decision);
        }

        if (decision.allowed()) {
            chain.doFilter(request, response);
            return;
        }

        // Log the key, never the whole request: keys are already the identifying dimension, and
        // logging bodies or full URLs on a path an attacker controls is how a rate limiter becomes a
        // log-volume amplifier.
        log.debug(
                "rate limited {} {} for key {} (retry after {})",
                request.getMethod(),
                rule.pattern(),
                key,
                decision.retryAfter());

        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setHeader("Retry-After", Long.toString(decision.retryAfterSeconds()));
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        // RFC 9457 problem detail, so clients get a machine-readable reason rather than a bare 429.
        response.getWriter()
                .write(
                        """
                        {"type":"https://ratelimit4j.io/errors/rate-limit-exceeded",\
                        "title":"Too Many Requests",\
                        "status":429,\
                        "detail":"Rate limit exceeded. Retry after %d seconds.",\
                        "retryAfterSeconds":%d}"""
                                .formatted(decision.retryAfterSeconds(), decision.retryAfterSeconds()));
    }

    private void writeRateLimitHeaders(HttpServletResponse response, RateLimitDecision decision) {
        response.setHeader("RateLimit-Limit", Long.toString(decision.limit()));
        response.setHeader("RateLimit-Remaining", Long.toString(decision.remaining()));
        response.setHeader("RateLimit-Reset", Long.toString(decision.retryAfterSeconds()));
    }

    /**
     * @return the first rule whose pattern matches, or null if none does
     */
    private RateLimitRule firstMatching(String path) {
        for (RateLimitRule rule : rules) {
            if (rule.matches(path)) {
                return rule;
            }
        }
        return null;
    }

    /**
     * @return the rules this filter applies, in evaluation order
     */
    public List<RateLimitRule> rules() {
        return rules;
    }
}
