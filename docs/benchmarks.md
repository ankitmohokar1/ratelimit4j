# Benchmarks

Measured with JMH. **These are from one developer laptop and are indicative only** — treat the
relative ordering as the signal and the absolute numbers as approximate.

```
Apple M-series, macOS 15, OpenJDK 22.0.1
JMH 1.37 · 1 fork · 3×2s warmup · 5×2s measurement · AverageTime, ns/op
```

Reproduce:

```bash
mvn -pl ratelimit4j-benchmarks -am package
java -jar ratelimit4j-benchmarks/target/benchmarks.jar
```

CI builds the harness but deliberately does not run it. Figures measured on contended shared runners
would be worse than no figures at all.

---

## Store strategy

The question: how should per-key read-modify-write be done? This benchmark exists because the store's
own design note asserted an answer, and it turned out to be half wrong.

| Strategy | all 8 threads on one key | spread over 1024 keys |
|---|---|---|
| CAS on immutable state | 2372 ± 232 | **82 ± 1** |
| `ConcurrentHashMap.compute` | 470 ± 19 | 222 ± 17 |
| One global lock | **410 ± 16** | 551 ± 55 |

**CAS is 5× worse on a single hot key.** The mechanism is a retry storm: with eight threads on one
cache line, most attempts lose the race and recompute, so the total work multiplies. A lock instead
makes each thread do the work exactly once and hands off in order. The cheaper the transition, the
worse this trade looks for CAS — and a rate limiter's transition is cheap.

**CAS is 2.7× better spread across keys.** Collisions are rare, so the retry path is almost never
taken and each update costs one uncontended CAS rather than a lock acquisition.

### What the library does with that

It keeps CAS, because the second column is the operating point: a limiter keyed by tenant, IP or API
token spreads over thousands of keys. The first column is a real weakness and it lands exactly when
one attacker is hammering one key. The mitigation, if it ever showed up in production, would be to
detect hot keys and route them through a striped lock — not to give up the 2.7× on everything else.

Two caveats on reading this table:

- The "one global lock" row is a naive baseline, not a well-built lock-based store. Its poor
  spread-case number is global serialisation across unrelated keys, not evidence that locks are slow.
  `ConcurrentHashMap.compute`, which locks per bin, is the fair comparator.
- Eight threads doing nothing but hammering one key with zero work in between is a deliberately
  extreme shape. Real gateway traffic does microseconds of work per request, so actual same-key
  contention is far lower than this.

---

## Per-decision cost

| Algorithm | uncontended | 8 threads, one key | 8 threads, 1024 keys |
|---|---|---|---|
| GCRA | 29.8 ± 0.8 | 2510 ± 193 | 791 ± 12 |
| Token bucket | 37.9 ± 1.6 | 2635 ± 228 | 693 ± 4 |
| Leaky bucket | 39.8 ± 1.8 | 3311 ± 92 | 751 ± 3 |
| Sliding window counter | 38.0 ± 0.4 | 2731 ± 250 | 689 ± 11 |
| Sliding window log | 31.9 ± 0.8 | 2372 ± 387 | 7342 ± 14315 |
| Fixed window | 27.2 ± 0.8 | 2555 ± 233 | 680 ± 5 |

### Reading this

**Uncontended, every algorithm costs 27–40 ns.** At that scale the choice between them is not a
performance decision — it is a decision about *semantics*: burst tolerance, boundary behaviour,
exactness. Choosing a fixed window over GCRA to save 2.6 ns, and accepting a 2× boundary spike for it,
would be a bad trade. Choose on the [algorithm guide](algorithms.md), not on this table.

**Contended on one key, the algorithm barely matters** — everything lands in 2.4–3.3 µs, because the
cost is the store's retry storm rather than the arithmetic. Same conclusion as the table above, and
the same mitigation.

**The sliding window log's spread number is both huge and wildly unstable** (±14315 on a mean of
7342). That is the algorithm being honest about itself: it copies an immutable array on every write,
so cost grows with the permit budget and varies with how full each key's log happens to be. It is
exactly why the class is documented as suitable only for small, high-value limits, and why its
constructor refuses budgets above 100k with an error saying so.

**The gap between uncontended (~30 ns) and spread-across-keys (~700 ns) is mostly cache, not
contention.** Walking 1024 keys across 8 threads misses cache on nearly every access, where the
uncontended benchmark hits the same L1-resident slot every time. The store benchmark above isolates
that: its spread case does the same map walk for 82 ns, so roughly 600 ns of the difference here is
the limiter's own work — policy arithmetic and allocating a `RateLimitDecision` per call — rather than
the store's.

Both figures are upper bounds on what a real deployment pays, since neither includes the microseconds
of actual request handling that separate consecutive limiter calls in production.
