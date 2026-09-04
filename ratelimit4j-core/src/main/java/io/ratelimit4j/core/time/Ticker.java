package io.ratelimit4j.core.time;

/**
 * A source of monotonically increasing nanosecond timestamps.
 *
 * <p>Every algorithm in this library reads time through a {@code Ticker} rather than calling
 * {@link System#nanoTime()} directly. That makes time an injected dependency, so tests can advance
 * the clock deterministically instead of sleeping — a rate limiter test suite that sleeps is both
 * slow and flaky.
 *
 * <p>Implementations must be monotonic and safe for concurrent use.
 */
@FunctionalInterface
public interface Ticker {

    /**
     * @return a monotonically non-decreasing timestamp in nanoseconds. The absolute value carries no
     *     meaning; only differences between two readings do.
     */
    long nanoTime();

    /**
     * @return a ticker backed by {@link System#nanoTime()}.
     */
    static Ticker system() {
        return System::nanoTime;
    }
}
