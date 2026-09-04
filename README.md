# ratelimit4j

Rate limiting algorithms for the JVM, with a lock-free in-process store, a distributed Redis backend,
and a Spring Boot gateway.

![Java](https://img.shields.io/badge/Java-17%2B-orange)
![License](https://img.shields.io/badge/License-Apache%202.0-blue)

> **Work in progress.** The core API and algorithms land first; the distributed backend, the Spring
> starter and the gateway follow. See the commit history.

## Why

Rate limiting looks solved until you write one. The hard parts are not the arithmetic:

- **Concurrency** — read-check-write on a shared counter passes every single-threaded test and hands
  out several times the budget the moment two threads hit the same key.
- **Time** — an algorithm that calls `System.nanoTime()` internally cannot be tested without sleeping.
- **Memory** — keyed by IP or token, a limiter accumulates state for every key it has ever seen.
- **Failure** — a limiter backed by Redis has put Redis on the critical path of every request.

This library is organised around those four.

## Usage

```java
RateLimiter limiter = Algorithm.GCRA.create(RateLimitPolicy.perMinute(100));

RateLimitDecision decision = limiter.tryAcquire("tenant-42");
if (decision.denied()) {
    response.setStatus(429);
    response.setHeader("Retry-After", Long.toString(decision.retryAfterSeconds()));
}
```

## Building

```bash
mvn verify
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
