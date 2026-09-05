package io.ratelimit4j.core.algorithm;

import static org.assertj.core.api.Assertions.assertThat;

import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.time.AdjustableTicker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The property that matters most: a limiter must not over-grant when many threads hit one key at once.
 *
 * <p>This is where a naive implementation fails. Read-check-write on a shared counter looks correct
 * in single-threaded tests and hands out several times the budget under contention, because every
 * thread reads the same pre-decrement value. These tests exist to make that failure mode impossible
 * to introduce unnoticed.
 *
 * <p>The clock is held still throughout. Frozen time means no permits can be earned during the run,
 * so the expected total is exactly the budget — an unambiguous assertion, rather than a bound that
 * has to be loosened to absorb refill during execution.
 */
class RateLimiterConcurrencyTest {

    private static final int THREADS = 32;
    private static final int ATTEMPTS_PER_THREAD = 500;

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @Timeout(30)
    @DisplayName("concurrent callers on one key are granted exactly the budget, never more")
    void doesNotOverGrantUnderContention(Algorithm algorithm) throws Exception {
        long budget = 1_000;
        AdjustableTicker frozen = new AdjustableTicker();
        RateLimiter limiter = algorithm.create(RateLimitPolicy.of(budget, Duration.ofHours(1)), frozen);

        long granted = hammer(() -> limiter.tryAcquire("hot-key").allowed() ? 1L : 0L);

        assertThat(granted)
                .as("%s over-granted under %d-way contention", algorithm, THREADS)
                .isEqualTo(budget);
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @Timeout(30)
    @DisplayName("multi-permit requests are all-or-nothing under contention")
    void multiPermitAcquisitionIsAtomic(Algorithm algorithm) throws Exception {
        long budget = 1_000;
        int permitsPerRequest = 4;
        AdjustableTicker frozen = new AdjustableTicker();
        RateLimiter limiter = algorithm.create(RateLimitPolicy.of(budget, Duration.ofHours(1)), frozen);

        long grantedRequests = hammer(() -> limiter.tryAcquire("hot-key", permitsPerRequest).allowed() ? 1L : 0L);

        // A partial grant would let the total permits handed out exceed the budget.
        assertThat(grantedRequests * permitsPerRequest)
                .as("%s granted %d permits against a budget of %d", algorithm, grantedRequests * permitsPerRequest, budget)
                .isLessThanOrEqualTo(budget);
        assertThat(grantedRequests)
                .as("%s should have granted the whole budget", algorithm)
                .isEqualTo(budget / permitsPerRequest);
    }

    @ParameterizedTest
    @EnumSource(Algorithm.class)
    @Timeout(30)
    @DisplayName("contention on one key does not corrupt the state of any other")
    void keysStayIsolatedUnderContention(Algorithm algorithm) throws Exception {
        long budget = 100;
        int keys = 16;
        AdjustableTicker frozen = new AdjustableTicker();
        RateLimiter limiter = algorithm.create(RateLimitPolicy.of(budget, Duration.ofHours(1)), frozen);

        AtomicLong[] grantsPerKey = IntStream.range(0, keys)
                .mapToObj(i -> new AtomicLong())
                .toArray(AtomicLong[]::new);

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                futures.add(pool.submit(() -> {
                    awaitQuietly(start);
                    for (int i = 0; i < ATTEMPTS_PER_THREAD; i++) {
                        int key = i % keys;
                        if (limiter.tryAcquire("key-" + key).allowed()) {
                            grantsPerKey[key].incrementAndGet();
                        }
                    }
                }));
            }

            start.countDown();
            for (Future<?> future : futures) {
                future.get(20, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        for (int key = 0; key < keys; key++) {
            assertThat(grantsPerKey[key].get())
                    .as("%s granted the wrong amount for key-%d", algorithm, key)
                    .isEqualTo(budget);
        }
    }

    @Test
    @Timeout(30)
    @DisplayName("peek stays non-destructive when it races against acquisitions")
    void peekDoesNotConsumeUnderContention() throws Exception {
        long budget = 500;
        AdjustableTicker frozen = new AdjustableTicker();
        RateLimiter limiter = Algorithm.GCRA.create(RateLimitPolicy.of(budget, Duration.ofHours(1)), frozen);

        // Half the threads only peek. If peeking consumed, the grant total would come in under budget.
        long granted = hammer(index -> index % 2 == 0
                ? (limiter.tryAcquire("hot-key").allowed() ? 1L : 0L)
                : peekAndDiscard(limiter));

        assertThat(granted).isEqualTo(budget);
    }

    private long peekAndDiscard(RateLimiter limiter) {
        limiter.peek("hot-key", 1);
        return 0L;
    }

    /** Runs {@code work} on every thread, all released together, and sums what they return. */
    private long hammer(Callable<Long> work) throws Exception {
        return hammer(index -> {
            try {
                return work.call();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    private long hammer(java.util.function.LongUnaryOperator work) throws Exception {
        AtomicLong total = new AtomicLong();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                final int threadIndex = t;
                futures.add(pool.submit(() -> {
                    // Release every thread at once, so the run actually contends rather than letting
                    // early threads finish before later ones have started.
                    awaitQuietly(start);
                    long local = 0;
                    for (int i = 0; i < ATTEMPTS_PER_THREAD; i++) {
                        local += work.applyAsLong(threadIndex);
                    }
                    total.addAndGet(local);
                }));
            }

            start.countDown();
            for (Future<?> future : futures) {
                future.get(20, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        return total.get();
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting to start", e);
        }
    }
}
