package io.ratelimit4j.core.store;

import io.ratelimit4j.core.time.Ticker;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongFunction;

/**
 * An in-process {@link KeyedStateStore} that is lock-free on the hot path and bounds its own memory.
 *
 * <h2>Concurrency</h2>
 *
 * Each key owns an {@link AtomicReference} to an immutable state object. {@link #apply} reads it,
 * runs the transition, and compare-and-swaps the result in, retrying if another thread won. Because
 * the state is immutable and the transition is pure, a lost race costs one wasted recomputation and
 * nothing else — there is no lock to contend on and no thread can block another.
 *
 * <p>Whether that beats {@code ConcurrentHashMap.compute}, which holds the bin lock for the duration
 * of the remapping function, depends entirely on the traffic shape — and not in the direction the
 * usual "lock-free is faster" intuition suggests. {@code StoreStrategyBenchmark} measures both shapes
 * at 8 threads (numbers from an M-series laptop; see {@code docs/benchmarks.md}):
 *
 * <table border="1">
 *   <caption>Nanoseconds per update, lower is better</caption>
 *   <tr><th></th><th>CAS</th><th>{@code compute}</th><th>one global lock</th></tr>
 *   <tr><td>All threads on one key</td><td>2372</td><td>470</td><td>410</td></tr>
 *   <tr><td>Spread over 1024 keys</td><td>82</td><td>222</td><td>551</td></tr>
 * </table>
 *
 * <p>CAS is <strong>five times worse</strong> on a single maximally-hot key. The mechanism is a retry
 * storm: with eight threads on one cache line, most attempts lose the race and recompute, so the
 * work multiplies, while a lock makes each thread do the work exactly once and hands off in an
 * orderly fashion. The cheaper the transition, the worse that trade looks for CAS.
 *
 * <p>It is nonetheless what this store does, because the second row is the operating point. A rate
 * limiter keyed by tenant, IP or API token spreads across thousands of keys, and there CAS is 2.7x
 * faster than {@code compute} — collisions are rare, so the retry path is almost never taken and each
 * update costs one uncontended CAS instead of a lock acquisition.
 *
 * <p>The honest caveat: a single genuinely hot key — one enormous tenant, or one attacker hammering
 * one token — lands in the first row, and this store is at its worst exactly when it is under attack.
 * If that shows up in production, the fix is to detect hot keys and route them through a striped lock
 * rather than to change the strategy wholesale, since doing so would give up the 2.7x on everything
 * else.
 *
 * <h2>Memory</h2>
 *
 * A rate limiter keyed by IP or API token accumulates state for every key it has ever seen, so an
 * unbounded map is a slow memory leak with a user-controlled growth rate. Two bounds apply:
 *
 * <ul>
 *   <li><strong>Retention.</strong> A key untouched for longer than the retention window is dropped.
 *       Its state is by then indistinguishable from a fresh one, so dropping it changes no decision.
 *   <li><strong>Capacity.</strong> Past {@code maxKeys}, the coldest keys are evicted to get back
 *       under the cap. This one <em>can</em> change a decision: evicting a key mid-window resets its
 *       budget. It is a backstop against unbounded growth under a key-space flooding attack, not a
 *       routine path — size it so it is not reached in normal operation.
 * </ul>
 *
 * <p>Maintenance runs inline, on whichever caller happens to trip the operation counter, and only one
 * thread sweeps at a time. That avoids owning a background thread — which a library embedded in
 * someone else's application should not do without being asked.
 *
 * <p>One race is accepted by design: a sweeper can remove a slot between another thread's read and
 * its CAS, discarding that write. The window is microseconds and only applies to keys already idle
 * for the whole retention period, so the worst outcome is one key briefly getting a fresh budget.
 * Closing it would need a lock on the hot path, which costs far more than the race does.
 *
 * @param <S> immutable per-key state
 */
public final class ConcurrentKeyedStateStore<S> implements KeyedStateStore<S> {

    /** Operations between maintenance sweeps. A power of two so the modulo folds to a mask. */
    private static final int MAINTENANCE_INTERVAL = 1024;

    private final ConcurrentHashMap<String, Slot<S>> slots = new ConcurrentHashMap<>();
    private final LongFunction<S> seed;
    private final Ticker ticker;
    private final long retentionNanos;
    private final int maxKeys;

    private final AtomicLong operations = new AtomicLong();
    private final AtomicBoolean sweeping = new AtomicBoolean();

    /**
     * @param seed builds the initial state for a key, given the current tick
     * @param ticker time source, shared with the algorithm using this store
     * @param retentionNanos how long an untouched key is kept. Must exceed the policy window, or
     *     active keys will be reset out from under themselves.
     * @param maxKeys hard cap on retained keys
     */
    public ConcurrentKeyedStateStore(LongFunction<S> seed, Ticker ticker, long retentionNanos, int maxKeys) {
        this.seed = Objects.requireNonNull(seed, "seed");
        this.ticker = Objects.requireNonNull(ticker, "ticker");
        if (retentionNanos <= 0) {
            throw new IllegalArgumentException("retentionNanos must be positive, got " + retentionNanos);
        }
        if (maxKeys <= 0) {
            throw new IllegalArgumentException("maxKeys must be positive, got " + maxKeys);
        }
        this.retentionNanos = retentionNanos;
        this.maxKeys = maxKeys;
    }

    @Override
    public <R> R apply(String key, long nowNanos, Function<S, Transition<S, R>> transition) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(transition, "transition");

        Slot<S> slot = slotFor(key, nowNanos);
        while (true) {
            S current = slot.state.get();
            Transition<S, R> next = transition.apply(current);
            if (slot.state.compareAndSet(current, next.nextState())) {
                slot.lastAccessNanos = nowNanos;
                maybeMaintain();
                return next.result();
            }
            // Lost the race. The winner's state is now visible; recompute against it.
            Thread.onSpinWait();
        }
    }

    @Override
    public S read(String key, long nowNanos) {
        return slotFor(Objects.requireNonNull(key, "key"), nowNanos).state.get();
    }

    @Override
    public int size() {
        return slots.size();
    }

    @Override
    public int evictExpired() {
        long cutoff = ticker.nanoTime() - retentionNanos;
        int removed = 0;
        for (Map.Entry<String, Slot<S>> entry : slots.entrySet()) {
            if (entry.getValue().lastAccessNanos < cutoff && slots.remove(entry.getKey(), entry.getValue())) {
                removed++;
            }
        }
        return removed;
    }

    private Slot<S> slotFor(String key, long nowNanos) {
        Slot<S> existing = slots.get(key);
        if (existing != null) {
            return existing;
        }
        // computeIfAbsent rather than putIfAbsent: seeding is cheap, but two threads racing on a cold
        // key must agree on one slot, or one of them writes into an orphan.
        return slots.computeIfAbsent(key, k -> new Slot<>(seed.apply(nowNanos), nowNanos));
    }

    private void maybeMaintain() {
        if ((operations.incrementAndGet() & (MAINTENANCE_INTERVAL - 1)) != 0) {
            return;
        }
        if (slots.size() < maxKeys / 2 && slots.size() < MAINTENANCE_INTERVAL) {
            return; // Too small to be worth scanning.
        }
        if (!sweeping.compareAndSet(false, true)) {
            return; // Another thread is already sweeping; one is enough.
        }
        try {
            evictExpired();
            if (slots.size() > maxKeys) {
                evictColdest(slots.size() - maxKeys);
            }
        } finally {
            sweeping.set(false);
        }
    }

    /**
     * Drops the {@code count} least recently used keys.
     *
     * <p>Approximate by construction: {@code lastAccessNanos} is read without synchronisation while
     * other threads keep writing, so the ordering is a snapshot that was already stale when taken.
     * For a capacity backstop that is the right trade — an exact LRU would need a lock or an
     * intrusive list touched on every request, and would cost more on the hot path than it saves here.
     */
    private void evictColdest(int count) {
        List<Map.Entry<String, Slot<S>>> byAge = new ArrayList<>(slots.entrySet());
        byAge.sort(Comparator.comparingLong(e -> e.getValue().lastAccessNanos));
        for (int i = 0; i < count && i < byAge.size(); i++) {
            slots.remove(byAge.get(i).getKey(), byAge.get(i).getValue());
        }
    }

    /**
     * One key's mutable cell.
     *
     * <p>{@code lastAccessNanos} is volatile rather than atomic: it is only ever read by the sweeper,
     * which tolerates a stale value, and racing writers all write approximately-now.
     */
    private static final class Slot<S> {
        final AtomicReference<S> state;
        volatile long lastAccessNanos;

        Slot(S initial, long nowNanos) {
            this.state = new AtomicReference<>(initial);
            this.lastAccessNanos = nowNanos;
        }
    }
}
