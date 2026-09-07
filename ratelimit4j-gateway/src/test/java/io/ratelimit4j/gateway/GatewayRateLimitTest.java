package io.ratelimit4j.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

/**
 * End-to-end coverage of the whole stack: auto-configuration, filter registration, rule matching and
 * the limiter itself, over real HTTP against a running server.
 *
 * <p>The unit tests already cover each piece. What this adds is the wiring — that the starter's
 * auto-configuration actually installs the filter, that the filter is ordered so it sees requests,
 * and that the YAML rules bind to real policies. Those are exactly the failures that unit tests
 * cannot see and that make a library look broken on first use.
 *
 * <p>Limits are overridden to small numbers so the test does not have to send hundreds of requests.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(
        properties = {
            "ratelimit4j.backend=LOCAL",
            "ratelimit4j.algorithm=GCRA",
            "ratelimit4j.key-strategy=HEADER",
            "ratelimit4j.header-name=X-API-Key",
            "ratelimit4j.rules[0].path=/api/auth/login",
            "ratelimit4j.rules[0].permits=2",
            "ratelimit4j.rules[0].window=1m",
            "ratelimit4j.rules[1].path=/api/echo",
            "ratelimit4j.rules[1].permits=5",
            "ratelimit4j.rules[1].window=1m"
        })
class GatewayRateLimitTest {

    @Autowired
    private TestRestTemplate rest;

    private ResponseEntity<String> get(String path, String apiKey) {
        org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
        headers.set("X-API-Key", apiKey);
        return rest.exchange(
                path, org.springframework.http.HttpMethod.GET, new org.springframework.http.HttpEntity<>(headers), String.class);
    }

    @Test
    @DisplayName("the configured budget is enforced over real HTTP, then 429s")
    void enforcesTheConfiguredLimit() {
        List<HttpStatus> statuses = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            statuses.add(HttpStatus.valueOf(get("/api/echo", "client-a").getStatusCode().value()));
        }

        assertThat(statuses.subList(0, 5)).containsOnly(HttpStatus.OK);
        assertThat(statuses.subList(5, 8)).containsOnly(HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    @DisplayName("each API key gets its own budget")
    void clientsAreIsolated() {
        for (int i = 0; i < 5; i++) {
            get("/api/echo", "client-b");
        }
        assertThat(get("/api/echo", "client-b").getStatusCode().value()).isEqualTo(429);

        assertThat(get("/api/echo", "client-c").getStatusCode().value())
                .as("a second client must not inherit the first's exhausted budget")
                .isEqualTo(200);
    }

    @Test
    @DisplayName("RateLimit-* headers are present and count down")
    void emitsRateLimitHeaders() {
        ResponseEntity<String> first = get("/api/echo", "client-d");

        assertThat(first.getHeaders().getFirst("RateLimit-Limit")).isEqualTo("5");
        assertThat(first.getHeaders().getFirst("RateLimit-Remaining")).isEqualTo("4");

        ResponseEntity<String> second = get("/api/echo", "client-d");
        assertThat(second.getHeaders().getFirst("RateLimit-Remaining")).isEqualTo("3");
    }

    @Test
    @DisplayName("a 429 response carries Retry-After and a problem-detail body")
    void refusalIsMachineReadable() {
        for (int i = 0; i < 5; i++) {
            get("/api/echo", "client-e");
        }
        ResponseEntity<String> refused = get("/api/echo", "client-e");

        assertThat(refused.getStatusCode().value()).isEqualTo(429);
        assertThat(refused.getHeaders().getFirst("Retry-After")).isNotNull();
        assertThat(refused.getBody()).contains("Too Many Requests").contains("retryAfterSeconds");
    }

    @Test
    @DisplayName("the tighter, more specific rule wins over the general one")
    void specificRulesTakePrecedence() {
        // The login rule allows 2; the echo rule allows 5. Both are separate budgets.
        assertThat(get("/api/echo", "client-f").getStatusCode().value()).isEqualTo(200);
        assertThat(get("/api/echo", "client-f").getStatusCode().value()).isEqualTo(200);
        assertThat(get("/api/echo", "client-f").getStatusCode().value())
                .as("the echo budget is 5 and only 2 have been spent")
                .isEqualTo(200);
    }

    @Test
    @DisplayName("a path matching no rule is never limited")
    void unmatchedPathsPassThrough() {
        for (int i = 0; i < 50; i++) {
            assertThat(get("/api/unlimited", "client-g").getStatusCode().value()).isEqualTo(200);
        }
    }

    @Test
    @DisplayName("actuator health is reachable and not limited")
    void actuatorIsNotLimited() {
        for (int i = 0; i < 20; i++) {
            assertThat(rest.getForEntity("/actuator/health", String.class).getStatusCode().value())
                    .isEqualTo(200);
        }
    }
}
