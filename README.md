# ratelimit4j

Six rate limiting algorithms, a lock-free in-process store, a distributed Redis backend, and a Spring
Boot gateway that puts them in front of real HTTP traffic.

[![CI](https://github.com/ankitmohokar1/ratelimit4j/actions/workflows/ci.yml/badge.svg)](https://github.com/ankitmohokar1/ratelimit4j/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-17%2B-orange)
![License](https://img.shields.io/badge/License-Apache%202.0-blue)

---

## Why this exists

Rate limiting looks like a solved problem until you write one. The parts that turn out to be hard are
not the arithmetic:

- **Concurrency.** The obvious read-check-write on a shared counter passes every single-threaded test
  and hands out several times the budget the moment two threads hit the same key.
- **Time.** An algorithm that calls `System.nanoTime()` internally cannot be tested without sleeping,
  so its test suite is slow, flaky, and quietly avoids the boundary cases that matter.
- **Memory.** Keyed by IP or API token, a limiter accumulates state for every key it has ever seen.
  That is a memory leak whose growth rate is controlled by whoever is attacking you.
- **Failure.** A limiter backed by Redis has put Redis on the critical path of every request. Deployed
  naively, it trades a rate limiting problem for an availability problem.

This library is organised around those four, and the design notes throughout say what was traded away
and why.

## Quick start

```xml
<dependency>
  <groupId>io.ratelimit4j</groupId>
  <artifactId>ratelimit4j-core</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

```java
RateLimiter limiter = Algorithm.GCRA.create(RateLimitPolicy.perMinute(100));

RateLimitDecision decision = limiter.tryAcquire("tenant-42");
if (decision.denied()) {
    response.setStatus(429);
    response.setHeader("Retry-After", Long.toString(decision.retryAfterSeconds()));
    return;
}
```

Tiered limits — the shape almost every real quota takes:

```java
// 10 per second protects the service; 1,000 per hour protects the quota.
RateLimiter limiter = TieredRateLimiter.of(
        Algorithm.GCRA.create(RateLimitPolicy.perSecond(10)),
        Algorithm.GCRA.create(RateLimitPolicy.perHour(1_000)));
```

Distributed, degrading to local rather than to an outage when Redis is unreachable:

```java
RateLimiter limiter = new ResilientRateLimiter(
        new RedisRateLimiter(connection, policy, Algorithm.GCRA, "rl:"),
        Algorithm.GCRA.create(policy),
        FailureMode.LOCAL_FALLBACK,
        5,
        Duration.ofSeconds(10));
```

## Choosing an algorithm

| Algorithm | State/key | Exact? | Bursts? | Use when |
|---|---|---|---|---|
| **GCRA** | 8 B | Yes | Yes | **Default.** Cheapest exact option; integer-only. |
| Token bucket | 16 B | Yes | Yes | You want the textbook model in the code. |
| Leaky bucket | 16 B | Yes | Yes | The token bucket's dual — see below. |
| Sliding window counter | 24 B | ~1% error | No | Large limits where smoothness matters. |
| Sliding window log | 8N B | Yes | No | Small, high-value limits: logins, password resets. |
| Fixed window | 16 B | 2× at boundary | No | Calendar-aligned quotas where the boundary is the point. |

Two things worth stating plainly, because both are commonly gotten wrong:

**The token bucket and the metered leaky bucket are the same algorithm.** One counts what remains and
refills; the other counts what is used and drains. Substituting `level = burst − tokens` turns either
into the other. Both are implemented here, and
[`LimiterEquivalenceTest`](ratelimit4j-core/src/test/java/io/ratelimit4j/core/algorithm/LimiterEquivalenceTest.java)
asserts they agree decision-for-decision over randomised traces. The genuine distinction is with the
*queueing* leaky bucket, which is a traffic shaper and does not fit a non-blocking interface.

**The fixed window's boundary spike is real and is 2×.** With a limit of 100/minute, a client that
sends 100 requests at 11:59:59 and 100 more at 12:00:01 has sent 200 in two seconds and broken no
rule.
[`WindowBoundaryBehaviourTest`](ratelimit4j-core/src/test/java/io/ratelimit4j/core/algorithm/WindowBoundaryBehaviourTest.java)
pins that down as expected behaviour, alongside the sliding algorithms refusing the same spike.

## Design

### Algorithms are pure functions

Every algorithm is a total function `(state, now, permits) -> (nextState, decision)`. No locking, no
clock access, no map lookups. Three things follow:

1. They are unit-testable by direct call, with hand-built states and no store involved.
2. The store can retry them freely under contention, because re-running them has no side effects.
3. The same transition ports to a Redis Lua script unchanged — the concurrency mechanism becomes
   "Redis is single-threaded" and nothing else moves.

### The store is lock-free — and the benchmark says that is a trade, not a win

[`ConcurrentKeyedStateStore`](ratelimit4j-core/src/main/java/io/ratelimit4j/core/store/ConcurrentKeyedStateStore.java)
gives each key an `AtomicReference` to an immutable state, and does read → transition → CAS, retrying
on a lost race.

The design note originally claimed this beats `ConcurrentHashMap.compute` under contention, on the
usual "lock-free is faster" reasoning. `StoreStrategyBenchmark` was written to demonstrate that, and
disproved half of it. Nanoseconds per update at 8 threads, lower is better:

| | all threads on one key | spread over 1024 keys |
|---|---|---|
| **CAS on immutable state** | 2372 | **82** |
| `ConcurrentHashMap.compute` | 470 | 222 |
| one global lock | **410** | 551 |

CAS is **5× worse** on a single maximally-hot key. The mechanism is a retry storm: with eight threads
on one cache line most attempts lose the race and recompute, multiplying the work, while a lock makes
each thread do the work once and hands off in order. The cheaper the transition, the worse that trade
looks for CAS.

The store still uses CAS, because the second column is the operating point — a limiter keyed by
tenant, IP or token spreads over thousands of keys, and there collisions are rare enough that the
retry path is almost never taken. But the first column is a genuine weakness, and it lands exactly
when one attacker is hammering one key. If it ever showed up in production the fix would be to detect
hot keys and route them through a striped lock, not to abandon the 2.7× on everything else.

(The "one global lock" row is the naive baseline, not a well-built lock-based store; its spread-case
number is global serialisation, not evidence that locks are slow. `compute` is the fair comparator.)

### Time is injected

Every algorithm reads time through a [`Ticker`](ratelimit4j-core/src/main/java/io/ratelimit4j/core/time/Ticker.java).
Tests advance an `AdjustableTicker` instead of sleeping, so the suite runs in under a second and can
exercise window rollover, refill and expiry exactly.

The store takes the caller's clock reading as a parameter rather than reading the clock itself. That
is not incidental: an earlier version let the store read its own clock when seeding a cold key, which
dated fresh state a few microseconds *after* the `now` the algorithm had already captured and skewed
every key's first decision by one permit. Frozen-ticker tests cannot see that bug — it took the
end-to-end HTTP test, against a real clock, to surface it.

### Memory is bounded

Keys are dropped once idle past a retention window (twice the policy window, floored at a minute), at
which point their state is indistinguishable from fresh. A hard `maxKeys` cap backstops key-space
flooding — an attacker rotating IPs or tokens to mint a new key per request. Maintenance runs inline
on whichever caller trips an operation counter, so the library owns no background thread.

### Failure is a design decision, not an accident

[`FailureMode`](ratelimit4j-redis/src/main/java/io/ratelimit4j/redis/FailureMode.java) makes the choice
explicit, because there is no safe default:

- `FAIL_OPEN` — a Redis outage must not become a service outage. Wrong if the limiter is the only
  thing standing between an attacker and what it protects.
- `FAIL_CLOSED` — exceeding the limit is worse than being down. Choose deliberately: it means a Redis
  failure takes the service with it.
- `LOCAL_FALLBACK` *(default)* — fall back to per-node limits. The fleet then allows up to N× the
  intended rate, which is a bounded, understood degradation rather than either extreme.

A circuit breaker opens after consecutive failures so a struggling Redis stops receiving load, and the
local limiter is kept warm during normal operation — a fallback that starts empty would hand every
client a full fresh budget at the worst possible moment.

## Modules

| Module | What it is |
|---|---|
| `ratelimit4j-core` | Algorithms, store, tiering. Zero runtime dependencies. |
| `ratelimit4j-redis` | Distributed limiters as atomic Lua scripts, plus the resilience wrapper. |
| `ratelimit4j-spring-boot-starter` | Auto-configuration, servlet filter, `RateLimit-*` headers. |
| `ratelimit4j-gateway` | A runnable gateway demonstrating all of it over HTTP. |
| `ratelimit4j-benchmarks` | JMH harness. |

## Running it

```bash
mvn verify                                                   # build + all tests
mvn -pl ratelimit4j-gateway -am spring-boot:run              # start the gateway

curl -i localhost:8080/api/echo                              # watch RateLimit-* headers
for i in $(seq 1 10); do curl -s -o /dev/null -w "%{http_code} " \
  -X POST localhost:8080/api/auth/login; done                # 200 ×5 then 429 ×5
```

Distributed mode:

```bash
docker compose up -d
mvn -pl ratelimit4j-gateway -am spring-boot:run -Dspring-boot.run.profiles=redis
```

Configuration lives under `ratelimit4j.*`; see
[`application.yml`](ratelimit4j-gateway/src/main/resources/application.yml) for a worked example.
Rules are evaluated in order and the first match wins, so specific paths must precede general ones —
reverse the two rules in that file and `/api/**` silently raises the login limit from 5 to 100.

## Testing

```
171 tests, all green without Docker (the Redis suite skips itself if no daemon is present).
```

Beyond the usual per-class tests, three suites carry most of the weight:

- **[`RateLimiterContractTest`](ratelimit4j-core/src/test/java/io/ratelimit4j/core/algorithm/RateLimiterContractTest.java)**
  runs one set of invariants against all six algorithms. Adding a seventh costs one line in the
  `Algorithm` enum and inherits every guarantee. This suite caught a real bug during development: the
  sliding window counter advertised a `retryAfter` of exactly one window, but its estimate is
  *continuous* across the boundary — the current count simply becomes the previous one — so a client
  that waited exactly as instructed was refused again.
- **[`RateLimiterConcurrencyTest`](ratelimit4j-core/src/test/java/io/ratelimit4j/core/algorithm/RateLimiterConcurrencyTest.java)**
  puts 32 threads × 500 attempts against one key with the clock frozen, and requires *exactly* the
  budget to be granted — not "roughly". Frozen time makes it an equality rather than a bound.
- **[`LimiterEquivalenceTest`](ratelimit4j-core/src/test/java/io/ratelimit4j/core/algorithm/LimiterEquivalenceTest.java)**
  replays randomised traces across implementations claimed to be equivalent and requires identical
  decisions at every step.

The Redis integration tests use Testcontainers against a real Redis, because the properties under test
are Redis's own — that `EVALSHA` is genuinely atomic, that `TIME` behaves as the scripts assume, that
TTLs expire keys. They skip rather than fail when no Docker daemon is available.

## Benchmarks

```bash
mvn -pl ratelimit4j-benchmarks -am package
java -jar ratelimit4j-benchmarks/target/benchmarks.jar
```

`AlgorithmBenchmark` measures per-decision cost uncontended, contended on one key, and spread across
keys. `StoreStrategyBenchmark` compares CAS-on-immutable-state against `ConcurrentHashMap.compute` and
a global lock — the benchmark that disproved half the design note above.

Measured numbers and methodology are in [docs/benchmarks.md](docs/benchmarks.md). They are from a
single developer laptop and are indicative only; CI builds the harness but deliberately does not run
it, because figures measured on contended shared runners would be worse than no figures at all.

## License

Apache 2.0 — see [LICENSE](LICENSE).
