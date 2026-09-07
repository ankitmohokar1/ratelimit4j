package io.ratelimit4j.spring;

import io.ratelimit4j.core.algorithm.Algorithm;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the rate limiting filter, bound from {@code ratelimit4j.*}.
 *
 * <pre>{@code
 * ratelimit4j:
 *   enabled: true
 *   algorithm: GCRA
 *   backend: REDIS
 *   key-strategy: HEADER
 *   header-name: X-API-Key
 *   redis:
 *     uri: redis://localhost:6379
 *     failure-mode: LOCAL_FALLBACK
 *   rules:
 *     - path: /api/auth/login
 *       permits: 5
 *       window: 1m
 *     - path: /api/**
 *       permits: 1000
 *       window: 1m
 *       burst: 1200
 * }</pre>
 */
@ConfigurationProperties(prefix = "ratelimit4j")
public class RateLimitProperties {

    /** Whether to install the filter at all. */
    private boolean enabled = true;

    /** Which algorithm the rules are enforced with. */
    private Algorithm algorithm = Algorithm.GCRA;

    /** Whether limits are per-node or shared through Redis. */
    private Backend backend = Backend.LOCAL;

    /** Which request dimension keys the limiter. */
    private KeyStrategy keyStrategy = KeyStrategy.REMOTE_ADDRESS;

    /** Header consulted when {@code keyStrategy} is {@code HEADER}. */
    private String headerName = "X-API-Key";

    /**
     * Whether to emit {@code RateLimit-*} headers on responses.
     *
     * <p>Worth leaving on: they let well-behaved clients self-throttle instead of discovering the
     * limit by being refused.
     */
    private boolean emitHeaders = true;

    /**
     * Rules, evaluated in order — the first whose path matches wins.
     *
     * <p>Order is significant and is the caller's responsibility: put specific paths before general
     * ones, or {@code /api/**} will shadow {@code /api/auth/login}.
     */
    private List<Rule> rules = new ArrayList<>();

    private Redis redis = new Redis();

    public enum Backend {
        /** Per-node limits. Simple and fast; the fleet-wide limit is N times the configured one. */
        LOCAL,
        /** One shared limit across the fleet, at the cost of a Redis round trip per request. */
        REDIS
    }

    public enum KeyStrategy {
        REMOTE_ADDRESS,
        /** Only safe behind a proxy that overwrites {@code X-Forwarded-For}. */
        FORWARDED_FOR,
        HEADER,
        PRINCIPAL
    }

    /** One path pattern and the policy applied to it. */
    public static class Rule {
        /** An Ant-style path pattern, e.g. {@code /api/**}. */
        private String path = "/**";

        private long permits = 100;
        private Duration window = Duration.ofMinutes(1);

        /** Instantaneous allowance. Defaults to {@code permits} when unset. */
        private Long burst;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public long getPermits() {
            return permits;
        }

        public void setPermits(long permits) {
            this.permits = permits;
        }

        public Duration getWindow() {
            return window;
        }

        public void setWindow(Duration window) {
            this.window = window;
        }

        public Long getBurst() {
            return burst;
        }

        public void setBurst(Long burst) {
            this.burst = burst;
        }
    }

    public static class Redis {
        private String uri = "redis://localhost:6379";
        private String keyPrefix = "ratelimit4j:";

        /** What to do when Redis is unreachable. See {@code FailureMode} for the trade-offs. */
        private String failureMode = "LOCAL_FALLBACK";

        /** Consecutive failures before the circuit breaker opens. */
        private int failureThreshold = 5;

        /** How long the breaker stays open before probing again. */
        private Duration breakerOpenDuration = Duration.ofSeconds(10);

        public String getUri() {
            return uri;
        }

        public void setUri(String uri) {
            this.uri = uri;
        }

        public String getKeyPrefix() {
            return keyPrefix;
        }

        public void setKeyPrefix(String keyPrefix) {
            this.keyPrefix = keyPrefix;
        }

        public String getFailureMode() {
            return failureMode;
        }

        public void setFailureMode(String failureMode) {
            this.failureMode = failureMode;
        }

        public int getFailureThreshold() {
            return failureThreshold;
        }

        public void setFailureThreshold(int failureThreshold) {
            this.failureThreshold = failureThreshold;
        }

        public Duration getBreakerOpenDuration() {
            return breakerOpenDuration;
        }

        public void setBreakerOpenDuration(Duration breakerOpenDuration) {
            this.breakerOpenDuration = breakerOpenDuration;
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    public Backend getBackend() {
        return backend;
    }

    public void setBackend(Backend backend) {
        this.backend = backend;
    }

    public KeyStrategy getKeyStrategy() {
        return keyStrategy;
    }

    public void setKeyStrategy(KeyStrategy keyStrategy) {
        this.keyStrategy = keyStrategy;
    }

    public String getHeaderName() {
        return headerName;
    }

    public void setHeaderName(String headerName) {
        this.headerName = headerName;
    }

    public boolean isEmitHeaders() {
        return emitHeaders;
    }

    public void setEmitHeaders(boolean emitHeaders) {
        this.emitHeaders = emitHeaders;
    }

    public List<Rule> getRules() {
        return rules;
    }

    public void setRules(List<Rule> rules) {
        this.rules = rules;
    }

    public Redis getRedis() {
        return redis;
    }

    public void setRedis(Redis redis) {
        this.redis = redis;
    }
}
