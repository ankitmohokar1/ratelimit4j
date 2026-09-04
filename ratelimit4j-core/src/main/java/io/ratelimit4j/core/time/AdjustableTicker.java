package io.ratelimit4j.core.time;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link Ticker} whose value only changes when the caller advances it.
 *
 * <p>Intended for tests and benchmarks: it lets a suite exercise refill, window rollover and expiry
 * behaviour in microseconds rather than by sleeping through real windows.
 *
 * <p>This class is thread-safe.
 */
public final class AdjustableTicker implements Ticker {

    private final AtomicLong nanos;

    /**
     * Creates a ticker starting at an arbitrary non-zero offset.
     *
     * <p>The offset is deliberately not zero: starting at zero hides bugs in code that treats a
     * {@code 0} timestamp as "unset".
     */
    public AdjustableTicker() {
        this(Duration.ofDays(1).toNanos());
    }

    public AdjustableTicker(long startNanos) {
        this.nanos = new AtomicLong(startNanos);
    }

    @Override
    public long nanoTime() {
        return nanos.get();
    }

    /**
     * Moves the clock forward.
     *
     * @param duration a non-negative amount of time
     * @return this ticker, for chaining
     * @throws IllegalArgumentException if {@code duration} is negative, which would break monotonicity
     */
    public AdjustableTicker advance(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Ticker must be monotonic; cannot advance by " + duration);
        }
        nanos.addAndGet(duration.toNanos());
        return this;
    }

    public AdjustableTicker advanceNanos(long deltaNanos) {
        if (deltaNanos < 0) {
            throw new IllegalArgumentException("Ticker must be monotonic; cannot advance by " + deltaNanos + "ns");
        }
        nanos.addAndGet(deltaNanos);
        return this;
    }
}
