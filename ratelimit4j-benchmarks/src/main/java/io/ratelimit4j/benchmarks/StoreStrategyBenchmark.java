package io.ratelimit4j.benchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures the three obvious ways to do per-key read-modify-write, on both traffic shapes that
 * matter: every thread on one hot key, and threads spread across a wide key space.
 *
 * <p>This benchmark exists because "lock-free is faster" is received wisdom, and received wisdom about
 * contention is often wrong. It is worth reading the numbers before believing the design note in
 * {@code ConcurrentKeyedStateStore} — including when they disagree with it.
 *
 * <p>All three strategies do the same amount of logical work and allocate the same way: each produces
 * a new immutable state object per update, which is what the real store does. The CAS loop includes
 * the same {@link Thread#onSpinWait()} the real implementation uses, so the comparison is against what
 * ships and not against a strawman.
 *
 * <pre>{@code
 * java -jar ratelimit4j-benchmarks/target/benchmarks.jar StoreStrategyBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Measurement(iterations = 5, time = 2)
@Warmup(iterations = 3, time = 2)
@State(Scope.Benchmark)
public class StoreStrategyBenchmark {

    /** Wide enough that threads rarely collide — the shape of a healthy multi-tenant deployment. */
    private static final int KEY_COUNT = 1024;

    private static final String HOT_KEY = "hot";

    /** Stands in for an algorithm's immutable state record. */
    private record Counter(long value) {
        Counter next() {
            return new Counter(value + 1);
        }
    }

    private ConcurrentHashMap<String, AtomicReference<Counter>> casMap;
    private ConcurrentHashMap<String, Counter> computeMap;
    private Map<String, Counter> synchronizedMap;
    private String[] keys;

    @Setup(Level.Trial)
    public void setUp() {
        keys = new String[KEY_COUNT];
        for (int i = 0; i < KEY_COUNT; i++) {
            keys[i] = "tenant-" + i;
        }

        casMap = new ConcurrentHashMap<>();
        computeMap = new ConcurrentHashMap<>();
        synchronizedMap = new HashMap<>();

        casMap.put(HOT_KEY, new AtomicReference<>(new Counter(0)));
        computeMap.put(HOT_KEY, new Counter(0));
        synchronizedMap.put(HOT_KEY, new Counter(0));
        for (String key : keys) {
            casMap.put(key, new AtomicReference<>(new Counter(0)));
            computeMap.put(key, new Counter(0));
            synchronizedMap.put(key, new Counter(0));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // One hot key: the pathological shape. One abusive client, or one very large tenant.
    // ---------------------------------------------------------------------------------------------

    @Benchmark
    @Threads(8)
    public void hotKey_cas(Blackhole blackhole) {
        blackhole.consume(cas(HOT_KEY));
    }

    @Benchmark
    @Threads(8)
    public void hotKey_compute(Blackhole blackhole) {
        blackhole.consume(computeMap.compute(HOT_KEY, (key, value) -> value.next()));
    }

    @Benchmark
    @Threads(8)
    public void hotKey_synchronized(Blackhole blackhole) {
        blackhole.consume(locked(HOT_KEY));
    }

    // ---------------------------------------------------------------------------------------------
    // Spread across keys: what healthy production traffic actually looks like.
    // ---------------------------------------------------------------------------------------------

    @Benchmark
    @Threads(8)
    public void spread_cas(Blackhole blackhole, Cursor cursor) {
        blackhole.consume(cas(keys[cursor.next() & (KEY_COUNT - 1)]));
    }

    @Benchmark
    @Threads(8)
    public void spread_compute(Blackhole blackhole, Cursor cursor) {
        blackhole.consume(computeMap.compute(keys[cursor.next() & (KEY_COUNT - 1)], (key, value) -> value.next()));
    }

    @Benchmark
    @Threads(8)
    public void spread_synchronized(Blackhole blackhole, Cursor cursor) {
        blackhole.consume(locked(keys[cursor.next() & (KEY_COUNT - 1)]));
    }

    private Counter cas(String key) {
        AtomicReference<Counter> slot = casMap.get(key);
        while (true) {
            Counter current = slot.get();
            Counter next = current.next();
            if (slot.compareAndSet(current, next)) {
                return next;
            }
            Thread.onSpinWait();
        }
    }

    /**
     * A single global lock, which is what a naive implementation ends up with. Note that this
     * serialises <em>every</em> key, not just the contended one — the cost that does not show up until
     * the key space is wide.
     */
    private Counter locked(String key) {
        synchronized (synchronizedMap) {
            Counter next = synchronizedMap.get(key).next();
            synchronizedMap.put(key, next);
            return next;
        }
    }

    /** Per-thread cursor, so threads walk the key space independently. */
    @State(Scope.Thread)
    public static class Cursor {
        private int cursor;

        public int next() {
            return cursor++;
        }
    }
}
