package io.ratelimit4j.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.ratelimit4j.core.RateLimitDecision;
import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.algorithm.Algorithm;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Exercises the Lua scripts against a real Redis.
 *
 * <p>Testcontainers rather than an embedded fake, because the properties under test are Redis's own:
 * that {@code EVALSHA} really is atomic under concurrency, that {@code TIME} returns what the scripts
 * assume, that TTLs actually expire keys. A fake that reimplements those would be testing the fake.
 *
 * <p>Skipped rather than failed when no Docker daemon is available, so a contributor without Docker
 * still gets a green {@code mvn verify}. CI has Docker, and runs them.
 */
class RedisRateLimiterIT {

    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7.4-alpine");

    private static GenericContainer<?> redis;
    private static RedisClient client;
    private static StatefulRedisConnection<String, String> connection;

    @BeforeAll
    static void startRedis() {
        assumeThat(DockerClientFactory.instance().isDockerAvailable())
                .as("Docker is required for the Redis integration tests")
                .isTrue();

        redis = new GenericContainer<>(REDIS_IMAGE).withExposedPorts(6379);
        redis.start();
        client = RedisClient.create("redis://" + redis.getHost() + ":" + redis.getMappedPort(6379));
        connection = client.connect();
    }

    @AfterAll
    static void stopRedis() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
        if (redis != null) {
            redis.stop();
        }
    }

    /** A fresh prefix per test, so tests cannot see one another's keys. */
    private RateLimiter limiter(Algorithm algorithm, RateLimitPolicy policy) {
        return new RedisRateLimiter(connection, policy, algorithm, "it:" + UUID.randomUUID() + ":");
    }

    @ParameterizedTest
    @EnumSource(
            value = Algorithm.class,
            names = {"GCRA", "TOKEN_BUCKET", "SLIDING_WINDOW_COUNTER"})
    @DisplayName("a fresh key gets exactly its budget and no more")
    void enforcesTheBudget(Algorithm algorithm) {
        RateLimiter limiter = limiter(algorithm, RateLimitPolicy.of(20, Duration.ofMinutes(5)));

        long granted = 0;
        for (int i = 0; i < 50; i++) {
            if (limiter.tryAcquire("alice").allowed()) {
                granted++;
            }
        }

        assertThat(granted).isEqualTo(20);
    }

    @ParameterizedTest
    @EnumSource(
            value = Algorithm.class,
            names = {"GCRA", "TOKEN_BUCKET", "SLIDING_WINDOW_COUNTER"})
    @DisplayName("keys are limited independently")
    void keysAreIsolated(Algorithm algorithm) {
        RateLimiter limiter = limiter(algorithm, RateLimitPolicy.of(5, Duration.ofMinutes(5)));

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
        }

        assertThat(limiter.tryAcquire("alice").denied()).isTrue();
        assertThat(limiter.tryAcquire("bob").allowed()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = Algorithm.class,
            names = {"GCRA", "TOKEN_BUCKET", "SLIDING_WINDOW_COUNTER"})
    @DisplayName("peek does not consume")
    void peekIsNonDestructive(Algorithm algorithm) {
        RateLimiter limiter = limiter(algorithm, RateLimitPolicy.of(5, Duration.ofMinutes(5)));

        for (int i = 0; i < 20; i++) {
            assertThat(limiter.peek("alice", 1).allowed()).isTrue();
        }

        long granted = 0;
        for (int i = 0; i < 10; i++) {
            if (limiter.tryAcquire("alice").allowed()) {
                granted++;
            }
        }
        assertThat(granted).isEqualTo(5);
    }

    @ParameterizedTest
    @EnumSource(
            value = Algorithm.class,
            names = {"GCRA", "TOKEN_BUCKET", "SLIDING_WINDOW_COUNTER"})
    @Timeout(60)
    @DisplayName("many client threads share one budget without over-granting")
    void isAtomicAcrossConcurrentClients(Algorithm algorithm) throws Exception {
        int budget = 200;
        int threads = 16;
        RateLimiter limiter = limiter(algorithm, RateLimitPolicy.of(budget, Duration.ofMinutes(5)));

        // Stands in for a fleet of gateway nodes hitting one shared Redis. If the script were not
        // atomic, the total would exceed the budget.
        AtomicLong granted = new AtomicLong();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                    for (int i = 0; i < 50; i++) {
                        if (limiter.tryAcquire("shared").allowed()) {
                            granted.incrementAndGet();
                        }
                    }
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(45, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(granted.get())
                .as("%s over-granted across %d concurrent clients", algorithm, threads)
                .isEqualTo(budget);
    }

    @Test
    @DisplayName("permits are recovered as the window elapses")
    void refillsOverTime() throws Exception {
        RateLimiter limiter = limiter(Algorithm.GCRA, RateLimitPolicy.of(10, Duration.ofSeconds(1)));

        for (int i = 0; i < 10; i++) {
            limiter.tryAcquire("alice");
        }
        assertThat(limiter.tryAcquire("alice").denied()).isTrue();

        // Redis owns the clock here, so this is a real wait — there is no ticker to advance.
        Thread.sleep(250);

        assertThat(limiter.tryAcquire("alice").allowed())
                .as("250ms at 10/s should have refilled about two permits")
                .isTrue();
    }

    @Test
    @DisplayName("a refusal reports a retryAfter that is actually sufficient")
    void retryAfterIsHonest() throws Exception {
        RateLimiter limiter = limiter(Algorithm.GCRA, RateLimitPolicy.of(5, Duration.ofSeconds(1)));

        for (int i = 0; i < 5; i++) {
            limiter.tryAcquire("alice");
        }
        RateLimitDecision denied = limiter.tryAcquire("alice");
        assertThat(denied.denied()).isTrue();
        assertThat(denied.retryAfter()).isPositive();

        Thread.sleep(denied.retryAfter().toMillis() + 20);

        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();
    }

    @Test
    @DisplayName("state survives a limiter instance being replaced, as it must across a deploy")
    void stateIsSharedAcrossLimiterInstances() {
        String prefix = "it:" + UUID.randomUUID() + ":";
        RateLimitPolicy policy = RateLimitPolicy.of(10, Duration.ofMinutes(5));
        RateLimiter nodeA = new RedisRateLimiter(connection, policy, Algorithm.GCRA, prefix);
        RateLimiter nodeB = new RedisRateLimiter(connection, policy, Algorithm.GCRA, prefix);

        for (int i = 0; i < 10; i++) {
            assertThat(nodeA.tryAcquire("alice").allowed()).isTrue();
        }

        assertThat(nodeB.tryAcquire("alice").denied())
                .as("a second node must see the budget the first one spent")
                .isTrue();
    }

    @Test
    @DisplayName("the script is re-uploaded transparently after Redis loses its script cache")
    void survivesAScriptFlush() {
        RateLimiter limiter = limiter(Algorithm.GCRA, RateLimitPolicy.of(100, Duration.ofMinutes(5)));

        assertThat(limiter.tryAcquire("alice").allowed()).isTrue();

        // Reproduces what a restart or a failover onto a cold replica does to EVALSHA.
        connection.sync().scriptFlush();

        assertThat(limiter.tryAcquire("alice").allowed())
                .as("a NOSCRIPT error must be recovered by falling back to EVAL")
                .isTrue();
    }

    @Test
    @DisplayName("idle keys are reclaimed by TTL rather than accumulating forever")
    void setsATtlOnEveryKey() {
        String prefix = "it:" + UUID.randomUUID() + ":";
        RateLimiter limiter =
                new RedisRateLimiter(connection, RateLimitPolicy.of(10, Duration.ofSeconds(1)), Algorithm.GCRA, prefix);

        limiter.tryAcquire("alice");

        Long ttl = connection.sync().pttl(prefix + "alice");
        assertThat(ttl)
                .as("a key with no TTL is a permanent leak, one per client ever seen")
                .isNotNull()
                .isGreaterThan(0L);
    }

    @Test
    void rejectsAlgorithmsWithNoRedisImplementation() {
        assertThat(RedisRateLimiter.supports(Algorithm.SLIDING_WINDOW_LOG)).isFalse();
        assertThat(RedisRateLimiter.supports(Algorithm.GCRA)).isTrue();
    }
}
