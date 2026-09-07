package io.ratelimit4j.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.algorithm.Algorithm;
import io.ratelimit4j.core.time.AdjustableTicker;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class RateLimitFilterTest {

    private final AdjustableTicker ticker = new AdjustableTicker();

    private RateLimitRule rule(String pattern, long permits) {
        return new RateLimitRule(
                pattern, Algorithm.GCRA.create(RateLimitPolicy.of(permits, Duration.ofMinutes(1)), ticker));
    }

    private MockHttpServletRequest get(String uri, String ip) {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", uri);
        request.setRequestURI(uri);
        request.setRemoteAddr(ip);
        return request;
    }

    /** Counts how many requests reached the application behind the filter. */
    private static final class CountingChain extends MockFilterChain {
        private final AtomicInteger passed = new AtomicInteger();

        @Override
        public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
            passed.incrementAndGet();
        }
    }

    @Test
    @DisplayName("requests within the limit reach the application")
    void allowsRequestsWithinTheLimit() throws Exception {
        RateLimitFilter filter =
                new RateLimitFilter(List.of(rule("/api/**", 3)), RateLimitKeyResolver.byRemoteAddress(), true);
        CountingChain chain = new CountingChain();

        for (int i = 0; i < 3; i++) {
            filter.doFilter(get("/api/orders", "10.0.0.1"), new MockHttpServletResponse(), chain);
        }

        assertThat(chain.passed.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("a refused request returns 429 and never reaches the application")
    void refusesOverTheLimit() throws Exception {
        RateLimitFilter filter =
                new RateLimitFilter(List.of(rule("/api/**", 2)), RateLimitKeyResolver.byRemoteAddress(), true);
        CountingChain chain = new CountingChain();

        for (int i = 0; i < 2; i++) {
            filter.doFilter(get("/api/orders", "10.0.0.1"), new MockHttpServletResponse(), chain);
        }
        MockHttpServletResponse refused = new MockHttpServletResponse();
        filter.doFilter(get("/api/orders", "10.0.0.1"), refused, chain);

        assertThat(refused.getStatus()).isEqualTo(429);
        assertThat(chain.passed.get())
                .as("the work a limiter saves is the work it declines to start")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("a 429 carries Retry-After and an RFC 9457 problem detail body")
    void refusalIsMachineReadable() throws Exception {
        RateLimitFilter filter =
                new RateLimitFilter(List.of(rule("/api/**", 1)), RateLimitKeyResolver.byRemoteAddress(), true);
        CountingChain chain = new CountingChain();

        filter.doFilter(get("/api/orders", "10.0.0.1"), new MockHttpServletResponse(), chain);
        MockHttpServletResponse refused = new MockHttpServletResponse();
        filter.doFilter(get("/api/orders", "10.0.0.1"), refused, chain);

        assertThat(refused.getHeader("Retry-After")).isNotNull();
        assertThat(Long.parseLong(refused.getHeader("Retry-After"))).isPositive();
        assertThat(refused.getContentType()).isEqualTo("application/problem+json");
        assertThat(refused.getContentAsString())
                .contains("\"status\":429")
                .contains("rate-limit-exceeded")
                .contains("retryAfterSeconds");
    }

    @Test
    @DisplayName("RateLimit-* headers are emitted on allowed responses so clients can self-throttle")
    void emitsRateLimitHeaders() throws Exception {
        RateLimitFilter filter =
                new RateLimitFilter(List.of(rule("/api/**", 10)), RateLimitKeyResolver.byRemoteAddress(), true);
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(get("/api/orders", "10.0.0.1"), response, new CountingChain());

        assertThat(response.getHeader("RateLimit-Limit")).isEqualTo("10");
        assertThat(response.getHeader("RateLimit-Remaining")).isEqualTo("9");
        assertThat(response.getHeader("RateLimit-Reset")).isNotNull();
    }

    @Test
    void headersCanBeSuppressed() throws Exception {
        RateLimitFilter filter =
                new RateLimitFilter(List.of(rule("/api/**", 10)), RateLimitKeyResolver.byRemoteAddress(), false);
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(get("/api/orders", "10.0.0.1"), response, new CountingChain());

        assertThat(response.getHeader("RateLimit-Limit")).isNull();
    }

    @Test
    @DisplayName("paths matching no rule are not limited at all")
    void unmatchedPathsAreNotLimited() throws Exception {
        RateLimitFilter filter =
                new RateLimitFilter(List.of(rule("/api/**", 1)), RateLimitKeyResolver.byRemoteAddress(), true);
        CountingChain chain = new CountingChain();

        for (int i = 0; i < 100; i++) {
            filter.doFilter(get("/health", "10.0.0.1"), new MockHttpServletResponse(), chain);
        }

        assertThat(chain.passed.get()).isEqualTo(100);
    }

    @Test
    @DisplayName("the first matching rule wins, so specific patterns must be listed first")
    void firstMatchingRuleWins() throws Exception {
        RateLimitFilter filter = new RateLimitFilter(
                List.of(rule("/api/auth/login", 2), rule("/api/**", 1000)),
                RateLimitKeyResolver.byRemoteAddress(),
                true);
        CountingChain chain = new CountingChain();

        for (int i = 0; i < 2; i++) {
            filter.doFilter(get("/api/auth/login", "10.0.0.1"), new MockHttpServletResponse(), chain);
        }
        MockHttpServletResponse refused = new MockHttpServletResponse();
        filter.doFilter(get("/api/auth/login", "10.0.0.1"), refused, chain);

        assertThat(refused.getStatus())
                .as("the tighter login rule must win over the general /api/** rule")
                .isEqualTo(429);

        // The general rule still applies elsewhere, and has its own budget.
        MockHttpServletResponse other = new MockHttpServletResponse();
        filter.doFilter(get("/api/orders", "10.0.0.1"), other, chain);
        assertThat(other.getStatus()).isEqualTo(200);
    }

    @Test
    @DisplayName("different clients are limited independently")
    void clientsAreIsolated() throws Exception {
        RateLimitFilter filter =
                new RateLimitFilter(List.of(rule("/api/**", 1)), RateLimitKeyResolver.byRemoteAddress(), true);
        CountingChain chain = new CountingChain();

        filter.doFilter(get("/api/orders", "10.0.0.1"), new MockHttpServletResponse(), chain);
        MockHttpServletResponse sameClient = new MockHttpServletResponse();
        filter.doFilter(get("/api/orders", "10.0.0.1"), sameClient, chain);
        MockHttpServletResponse otherClient = new MockHttpServletResponse();
        filter.doFilter(get("/api/orders", "10.0.0.2"), otherClient, chain);

        assertThat(sameClient.getStatus()).isEqualTo(429);
        assertThat(otherClient.getStatus()).isEqualTo(200);
    }

    @Test
    @DisplayName("header keying puts requests with no header into one shared anonymous bucket")
    void anonymousRequestsShareABucket() throws Exception {
        RateLimitFilter filter =
                new RateLimitFilter(List.of(rule("/api/**", 2)), RateLimitKeyResolver.byHeader("X-API-Key"), true);
        CountingChain chain = new CountingChain();

        // Three requests with no key at all. If omitting the header granted a fresh bucket each time,
        // the header would be a way around the limit rather than the basis for it.
        for (int i = 0; i < 2; i++) {
            filter.doFilter(get("/api/orders", "10.0.0." + i), new MockHttpServletResponse(), chain);
        }
        MockHttpServletResponse third = new MockHttpServletResponse();
        filter.doFilter(get("/api/orders", "10.0.0.9"), third, chain);

        assertThat(third.getStatus()).isEqualTo(429);
    }

    @Test
    void headerKeyingSeparatesDistinctApiKeys() throws Exception {
        RateLimitFilter filter =
                new RateLimitFilter(List.of(rule("/api/**", 1)), RateLimitKeyResolver.byHeader("X-API-Key"), true);
        CountingChain chain = new CountingChain();

        MockHttpServletRequest first = get("/api/orders", "10.0.0.1");
        first.addHeader("X-API-Key", "key-a");
        filter.doFilter(first, new MockHttpServletResponse(), chain);

        MockHttpServletRequest second = get("/api/orders", "10.0.0.1");
        second.addHeader("X-API-Key", "key-b");
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(second, response, chain);

        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    @DisplayName("the budget recovers as the window elapses")
    void budgetRecoversOverTime() throws Exception {
        RateLimitFilter filter =
                new RateLimitFilter(List.of(rule("/api/**", 60)), RateLimitKeyResolver.byRemoteAddress(), true);
        CountingChain chain = new CountingChain();

        for (int i = 0; i < 60; i++) {
            filter.doFilter(get("/api/orders", "10.0.0.1"), new MockHttpServletResponse(), chain);
        }
        MockHttpServletResponse refused = new MockHttpServletResponse();
        filter.doFilter(get("/api/orders", "10.0.0.1"), refused, chain);
        assertThat(refused.getStatus()).isEqualTo(429);

        ticker.advance(Duration.ofSeconds(30));

        MockHttpServletResponse afterWait = new MockHttpServletResponse();
        filter.doFilter(get("/api/orders", "10.0.0.1"), afterWait, chain);
        assertThat(afterWait.getStatus()).isEqualTo(200);
    }

    @Test
    void forwardedForResolverPrefersTheLeftMostEntry() {
        MockHttpServletRequest request = get("/api/orders", "10.0.0.1");
        request.addHeader("X-Forwarded-For", "203.0.113.7, 70.41.3.18, 150.172.238.178");

        HttpServletRequest asServletRequest = request;
        assertThat(RateLimitKeyResolver.byTrustedForwardedFor().resolve(asServletRequest))
                .isEqualTo("203.0.113.7");
    }

    @Test
    void forwardedForResolverFallsBackToThePeerAddress() {
        assertThat(RateLimitKeyResolver.byTrustedForwardedFor().resolve(get("/api/orders", "10.0.0.5")))
                .isEqualTo("10.0.0.5");
    }
}
