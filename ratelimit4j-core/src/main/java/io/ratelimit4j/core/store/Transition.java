package io.ratelimit4j.core.store;

/**
 * The result of applying an algorithm to one key's state: the state that should replace it, and the
 * value to hand back to the caller.
 *
 * <p>Modelling each algorithm as {@code (state, now) -> (nextState, decision)} is what lets this
 * library share one concurrency strategy across six algorithms. The algorithm itself is a pure
 * function with no locking, no clock access and no map lookups, so:
 *
 * <ul>
 *   <li>it can be unit tested by calling it directly with a hand-built state;
 *   <li>the store can retry it freely under contention, because re-running it has no side effects;
 *   <li>the same transition translates directly into a Redis Lua script, where the "store" is a hash
 *       and the retry loop is replaced by Redis's single-threaded execution.
 * </ul>
 *
 * @param nextState state to install for the key
 * @param result value returned to the caller
 * @param <S> the algorithm's state type
 * @param <R> the caller-visible result type
 */
public record Transition<S, R>(S nextState, R result) {

    public static <S, R> Transition<S, R> of(S nextState, R result) {
        return new Transition<>(nextState, result);
    }
}
