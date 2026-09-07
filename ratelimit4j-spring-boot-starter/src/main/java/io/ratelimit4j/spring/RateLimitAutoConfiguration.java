package io.ratelimit4j.spring;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.time.Ticker;
import io.ratelimit4j.redis.FailureMode;
import io.ratelimit4j.redis.RedisRateLimiter;
import io.ratelimit4j.redis.ResilientRateLimiter;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;

/**
 * Wires the filter, its rules and the chosen backend from {@code ratelimit4j.*} configuration.
 *
 * <p>Every bean here is {@code @ConditionalOnMissingBean}, so an application that needs something the
 * properties cannot express — a key resolver keyed on tenant plus endpoint, a rule set loaded from a
 * database — can define its own bean and keep the rest of the auto-configuration. A starter that
 * cannot be partially overridden ends up being replaced wholesale.
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "ratelimit4j", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RateLimitProperties.class)
public class RateLimitAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(RateLimitAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public Ticker rateLimitTicker() {
        return Ticker.system();
    }

    @Bean
    @ConditionalOnMissingBean
    public RateLimitKeyResolver rateLimitKeyResolver(RateLimitProperties properties) {
        return switch (properties.getKeyStrategy()) {
            case REMOTE_ADDRESS -> RateLimitKeyResolver.byRemoteAddress();
            case FORWARDED_FOR -> RateLimitKeyResolver.byTrustedForwardedFor();
            case HEADER -> RateLimitKeyResolver.byHeader(properties.getHeaderName());
            case PRINCIPAL -> RateLimitKeyResolver.byPrincipal();
        };
    }

    /**
     * Builds one limiter per rule.
     *
     * <p>Separate limiters rather than one shared instance, because each rule has its own policy and
     * therefore its own state. They do share a key space per rule only, so a client's budget on
     * {@code /api/auth/login} is independent of its budget on {@code /api/**} — which is the point of
     * having separate rules at all.
     */
    @Bean
    @ConditionalOnMissingBean
    public List<RateLimitRule> rateLimitRules(
            RateLimitProperties properties,
            Ticker ticker,
            ObjectProvider<StatefulRedisConnection<String, String>> redisConnection) {

        List<RateLimitRule> rules = new ArrayList<>();
        for (RateLimitProperties.Rule configured : properties.getRules()) {
            RateLimitPolicy policy = new RateLimitPolicy(
                    configured.getPermits(),
                    configured.getWindow(),
                    configured.getBurst() == null ? configured.getPermits() : configured.getBurst());
            rules.add(new RateLimitRule(configured.getPath(), buildLimiter(properties, policy, ticker, redisConnection)));
        }

        if (rules.isEmpty()) {
            log.info("ratelimit4j is enabled but no rules are configured; no requests will be limited");
        } else {
            log.info("ratelimit4j enforcing {} rule(s) with {} via {}", rules.size(), properties.getAlgorithm(), properties.getBackend());
        }
        return rules;
    }

    private RateLimiter buildLimiter(
            RateLimitProperties properties,
            RateLimitPolicy policy,
            Ticker ticker,
            ObjectProvider<StatefulRedisConnection<String, String>> redisConnection) {

        RateLimiter local = properties.getAlgorithm().create(policy, ticker);
        if (properties.getBackend() == RateLimitProperties.Backend.LOCAL) {
            return local;
        }

        StatefulRedisConnection<String, String> connection = redisConnection.getIfAvailable();
        if (connection == null) {
            // Better to say so loudly at startup than to silently enforce a per-node limit that
            // operators believe is fleet-wide.
            throw new IllegalStateException(
                    "ratelimit4j.backend=REDIS but no Redis connection bean is available. Provide a "
                            + "StatefulRedisConnection bean, or set ratelimit4j.backend=LOCAL.");
        }

        RateLimiter distributed = new RedisRateLimiter(
                connection, policy, properties.getAlgorithm(), properties.getRedis().getKeyPrefix());

        return new ResilientRateLimiter(
                distributed,
                local,
                FailureMode.valueOf(properties.getRedis().getFailureMode()),
                properties.getRedis().getFailureThreshold(),
                properties.getRedis().getBreakerOpenDuration());
    }

    @Bean
    @ConditionalOnMissingBean
    public FilterRegistrationBean<RateLimitFilter> rateLimitFilterRegistration(
            List<RateLimitRule> rules, RateLimitKeyResolver keyResolver, RateLimitProperties properties) {

        FilterRegistrationBean<RateLimitFilter> registration = new FilterRegistrationBean<>(
                new RateLimitFilter(rules, keyResolver, properties.isEmitHeaders()));

        // Keying by principal needs authentication to have run; every other strategy is better off
        // ahead of it, so that refused traffic costs as little as possible.
        registration.setOrder(
                properties.getKeyStrategy() == RateLimitProperties.KeyStrategy.PRINCIPAL
                        ? RateLimitFilter.DEFAULT_ORDER + 400
                        : RateLimitFilter.DEFAULT_ORDER);
        registration.addUrlPatterns("/*");
        return registration;
    }

    /**
     * Opens a Redis connection from {@code ratelimit4j.redis.uri} when the Redis backend is selected
     * and the application has not supplied its own.
     */
    @AutoConfiguration
    @ConditionalOnClass(RedisClient.class)
    @ConditionalOnProperty(prefix = "ratelimit4j", name = "backend", havingValue = "REDIS")
    public static class RedisConnectionConfiguration {

        @Bean(destroyMethod = "close")
        @ConditionalOnMissingBean
        public StatefulRedisConnection<String, String> ratelimit4jRedisConnection(RateLimitProperties properties) {
            return RedisClient.create(properties.getRedis().getUri()).connect();
        }
    }
}
