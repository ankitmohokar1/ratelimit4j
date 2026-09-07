package io.ratelimit4j.benchmarks;

import io.ratelimit4j.core.RateLimitPolicy;
import io.ratelimit4j.core.RateLimiter;
import io.ratelimit4j.core.algorithm.Algorithm;
import io.ratelimit4j.core.time.Ticker;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Per-request cost of each algorithm, uncontended and under contention.
 *
 * <p>What these are for: rate limiters run on the hot path of every request, so their own cost is
 * paid by every request they allow as well as every one they refuse. An algorithm that is elegant but
 * costs microseconds is a worse choice than a slightly cruder one that costs tens of nanoseconds.
 *
 * <p>Budgets are set enormous on purpose. The interesting number is the cost of <em>deciding</em>,
 * and a limiter that spends the benchmark refusing is measuring the rejection path — which is cheaper
 * and not what production spends its time doing.
 *
 * <pre>{@code
 * mvn -pl ratelimit4j-benchmarks -am package
 * java -jar ratelimit4j-benchmarks/target/benchmarks.jar AlgorithmBenchmark
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Measurement(iterations = 5, time = 1)
@org.openjdk.jmh.annotations.Warmup(iterations = 3, time = 1)
@State(Scope.Benchmark)
public class AlgorithmBenchmark {

    @Param({"GCRA", "TOKEN_BUCKET", "LEAKY_BUCKET", "SLIDING_WINDOW_COUNTER", "SLIDING_WINDOW_LOG", "FIXED_WINDOW"})
    public Algorithm algorithm;

    private RateLimiter limiter;
    private String[] keys;

    @Setup(Level.Trial)
    public void setUp() {
        // The log algorithm keeps one timestamp per permit, so its budget is capped separately; giving
        // it the same enormous budget as the others would measure allocation, not the algorithm.
        long permits = algorithm == Algorithm.SLIDING_WINDOW_LOG ? 10_000 : 1_000_000_000L;
        limiter = algorithm.create(RateLimitPolicy.of(permits, Duration.ofHours(1)), Ticker.system());

        // A spread of keys, so the measurement includes the map lookup a real deployment pays and not
        // just a single perfectly-cached slot.
        keys = new String[1024];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = "tenant-" + i;
        }
    }

    /** Single-threaded cost: the floor, with no contention at all. */
    @Benchmark
    @Threads(1)
    public void uncontended(Blackhole blackhole) {
        blackhole.consume(limiter.tryAcquire(keys[0], 1));
    }

    /**
     * The pathological case: every thread hammering one key.
     *
     * <p>This is where a lock-based implementation collapses and a CAS-based one degrades gracefully,
     * and it is not hypothetical — one hot tenant or one abusive client produces exactly this shape.
     */
    @Benchmark
    @Threads(8)
    public void contendedOnOneKey(Blackhole blackhole) {
        blackhole.consume(limiter.tryAcquire(keys[0], 1));
    }

    /** The realistic case: many threads spread across many keys. */
    @Benchmark
    @Threads(8)
    public void contendedAcrossKeys(Blackhole blackhole, ThreadIndex index) {
        blackhole.consume(limiter.tryAcquire(keys[index.next() & (keys.length - 1)], 1));
    }

    /** Per-thread key cursor, so threads walk the key space independently. */
    @State(Scope.Thread)
    public static class ThreadIndex {
        private int cursor;

        public int next() {
            return cursor++;
        }
    }
}
