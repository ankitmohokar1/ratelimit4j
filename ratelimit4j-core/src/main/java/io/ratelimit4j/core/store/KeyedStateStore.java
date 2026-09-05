package io.ratelimit4j.core.store;

import java.util.function.Function;

/**
 * Per-key state with atomic read-modify-write, plus a bound on how long unused keys are retained.
 *
 * <p>Separating this from the algorithms is what makes the algorithms portable: swap the
 * implementation for a Redis-backed one and the same {@link Transition} function enforces the same
 * policy across a fleet.
 *
 * @param <S> the per-key state type. Implementations require it to be immutable — they may hand the
 *     same instance to several concurrent {@code apply} attempts.
 */
public interface KeyedStateStore<S> {

    /**
     * Atomically applies {@code transition} to the state stored under {@code key}, installing the
     * returned next state and handing back the returned result.
     *
     * <p>If the key has no state, the store seeds one at {@code nowNanos}.
     *
     * <p>{@code transition} may be invoked more than once for a single call when another thread wins a
     * race, so it must be free of side effects.
     *
     * @param nowNanos the caller's clock reading for this decision. Passed in rather than read by the
     *     store, so that the instant used to seed a cold key is exactly the instant the algorithm is
     *     reasoning about. A store that read the clock itself would seed a fresh key a few microseconds
     *     <em>after</em> the {@code now} the algorithm had already captured, leaving the new state
     *     dated in the algorithm's future and skewing its very first decision.
     */
    <R> R apply(String key, long nowNanos, Function<S, Transition<S, R>> transition);

    /**
     * Reads the current state for {@code key}, seeding it at {@code nowNanos} if absent, without
     * writing anything back.
     */
    S read(String key, long nowNanos);

    /**
     * @return the number of keys currently retained, including any that are expired but not yet
     *     reclaimed
     */
    int size();

    /**
     * Drops every key whose last write is older than the retention window.
     *
     * @return the number of keys reclaimed
     */
    int evictExpired();
}
