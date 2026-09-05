package io.ratelimit4j.core.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.ratelimit4j.core.time.AdjustableTicker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class ConcurrentKeyedStateStoreTest {

    private static final long RETENTION = Duration.ofMinutes(1).toNanos();

    /** One clock for the whole class, so every call site supplies a coherent reading. */
    private final AdjustableTicker ticker = new AdjustableTicker();

    private ConcurrentKeyedStateStore<Long> store() {
        return store(1_000_000);
    }

    private ConcurrentKeyedStateStore<Long> store(int maxKeys) {
        return new ConcurrentKeyedStateStore<>(now -> 0L, ticker, RETENTION, maxKeys);
    }

    private <R> R apply(
            ConcurrentKeyedStateStore<Long> store, String key, Function<Long, Transition<Long, R>> transition) {
        return store.apply(key, ticker.nanoTime(), transition);
    }

    private long read(ConcurrentKeyedStateStore<Long> store, String key) {
        return store.read(key, ticker.nanoTime());
    }

    @Test
    void seedsAKeyOnFirstAccess() {
        ConcurrentKeyedStateStore<Long> store = store();

        assertThat(read(store, "alice")).isZero();
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("a cold key is seeded at the caller's tick, not at one the store read for itself")
    void seedsAtTheCallersTick() {
        ConcurrentKeyedStateStore<Long> store =
                new ConcurrentKeyedStateStore<>(now -> now, ticker, RETENTION, 1_000);

        long callerTick = ticker.nanoTime() - Duration.ofSeconds(5).toNanos();
        long seeded = store.read("alice", callerTick);

        // A store that read its own clock would seed this key ~5s later than the caller believes,
        // dating fresh state in the algorithm's future and skewing its very first decision.
        assertThat(seeded).isEqualTo(callerTick);
    }

    @Test
    void applyInstallsTheNextStateAndReturnsTheResult() {
        ConcurrentKeyedStateStore<Long> store = store();

        String result = apply(store, "alice", count -> Transition.of(count + 5, "was " + count));

        assertThat(result).isEqualTo("was 0");
        assertThat(read(store, "alice")).isEqualTo(5L);
    }

    @Test
    void keysDoNotShareState() {
        ConcurrentKeyedStateStore<Long> store = store();

        apply(store, "alice", count -> Transition.of(count + 1, count));
        apply(store, "bob", count -> Transition.of(count + 100, count));

        assertThat(read(store, "alice")).isEqualTo(1L);
        assertThat(read(store, "bob")).isEqualTo(100L);
    }

    @Test
    @Timeout(30)
    @DisplayName("every concurrent increment lands — no update is lost to a race")
    void appliesEveryConcurrentUpdate() throws Exception {
        int threads = 16;
        int incrementsPerThread = 10_000;
        ConcurrentKeyedStateStore<Long> store = store();

        runConcurrently(threads, () -> {
            for (int i = 0; i < incrementsPerThread; i++) {
                apply(store, "hot", count -> Transition.of(count + 1, null));
            }
        });

        // A lost-update bug shows up here as a total below the expected one.
        assertThat(read(store, "hot")).isEqualTo((long) threads * incrementsPerThread);
    }

    @Test
    @Timeout(30)
    @DisplayName("racing threads seeding the same cold key converge on one slot")
    void coldKeyRaceProducesOneSlot() throws Exception {
        int threads = 32;
        ConcurrentKeyedStateStore<Long> store = store();

        runConcurrently(threads, () -> apply(store, "cold", count -> Transition.of(count + 1, null)));

        assertThat(store.size()).isEqualTo(1);
        assertThat(read(store, "cold"))
                .as("a thread that seeded an orphan slot would have lost its increment")
                .isEqualTo((long) threads);
    }

    @Test
    @DisplayName("keys idle beyond the retention window are reclaimed")
    void evictsKeysPastRetention() {
        ConcurrentKeyedStateStore<Long> store = store();

        apply(store, "stale", count -> Transition.of(count + 1, null));
        ticker.advanceNanos(RETENTION / 2);
        apply(store, "fresh", count -> Transition.of(count + 1, null));

        // Far enough that "stale" is past retention but "fresh" is not.
        ticker.advanceNanos(RETENTION / 2 + 1);

        assertThat(store.evictExpired()).isEqualTo(1);
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    void evictionLeavesActiveKeysAlone() {
        ConcurrentKeyedStateStore<Long> store = store();

        apply(store, "busy", count -> Transition.of(count + 1, null));
        ticker.advanceNanos(RETENTION / 2);

        assertThat(store.evictExpired()).isZero();
        assertThat(read(store, "busy")).isEqualTo(1L);
    }

    @Test
    @DisplayName("the key count stays bounded under a flood of one-shot keys")
    void boundsMemoryUnderKeySpaceFlooding() {
        int maxKeys = 2_048;
        ConcurrentKeyedStateStore<Long> store = store(maxKeys);

        // Simulates an attacker rotating the limiter key — a fresh IP or token on every request.
        for (int i = 0; i < 100_000; i++) {
            apply(store, "attacker-" + i, count -> Transition.of(count + 1, null));
        }

        assertThat(store.size())
                .as("an unbounded map would be holding all 100,000 keys here")
                .isLessThanOrEqualTo(maxKeys + 1024);
    }

    @Test
    void rejectsNonPositiveRetention() {
        assertThatThrownBy(() -> new ConcurrentKeyedStateStore<Long>(now -> 0L, ticker, 0, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("retentionNanos must be positive");
    }

    @Test
    void rejectsNonPositiveMaxKeys() {
        assertThatThrownBy(() -> new ConcurrentKeyedStateStore<Long>(now -> 0L, ticker, RETENTION, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxKeys must be positive");
    }

    /** Runs {@code work} on {@code threads} threads, all released together. */
    private void runConcurrently(int threads, Runnable work) throws Exception {
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
                        throw new AssertionError("interrupted while waiting to start", e);
                    }
                    work.run();
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(20, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
