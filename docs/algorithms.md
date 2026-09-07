# Choosing a rate limiting algorithm

The short version is in the [README](../README.md). This is the longer one: what each algorithm
actually guarantees, what it costs, and the cases where the difference between them decides an
incident.

## The question the algorithm answers

"100 requests per minute" is ambiguous, and every algorithm here resolves the ambiguity differently:

- May all 100 arrive in the same millisecond? (Burst.)
- Is the minute a fixed calendar minute, or the trailing 60 seconds? (Window shape.)
- Is 101 in 60 seconds definitely refused, or only probably? (Exactness.)

Picking an algorithm is picking answers to those three. Everything below follows from them.

---

## GCRA — the default

**State:** one `long`. **Exact:** yes. **Bursts:** yes.

The theoretical arrival time (TAT) is the instant the bucket would next be empty. Each permit costs
one emission interval `T = window / permits`; a request is admitted when the TAT it would produce is
no further ahead than the delay tolerance `τ = T × burst`.

```
tat'  = max(tat, now) + permits × T
allow ⟺ tat' − τ ≤ now
```

Behaviourally identical to a token bucket, but with half the state and no floating point, which is why
Redis implementations of rate limiting converge on it. A single 64-bit value is a single CAS, or a
single Redis `SET`, with no multi-field consistency to preserve.

The cost is legibility: "remaining" has to be reconstructed from how far the TAT sits behind `now`,
rather than read off a counter. If you are going to be woken at 3am to reason about this, that matters
more than eight bytes — which is the honest argument for the token bucket.

**Use it unless you have a specific reason not to.**

## Token bucket

**State:** a `double` and a `long`. **Exact:** yes. **Bursts:** yes.

Tokens accrue at the sustained rate up to `burst`; a request spending `n` tokens succeeds if `n` are
present. Refill is lazy — computed from elapsed time on each request — so an idle key costs nothing
and the algorithm is O(1) regardless of how long it sat untouched.

The fractional token count drifts, however slightly. At realistic rates the error stays many orders of
magnitude below one token, but GCRA does not have the problem at all.

**Use it when the code will be read more often than it is optimised.**

## Leaky bucket (metered)

**State:** a `double` and a `long`. **Exact:** yes. **Bursts:** yes.

Each permit adds to a bucket that drains at a constant rate; a request is refused when it would
overflow.

**This is the token bucket.** Substituting `level = burst − tokens` turns either into the other. Both
are implemented here, and `LimiterEquivalenceTest` asserts they agree decision-for-decision over
randomised traces. Interview answers and design docs routinely present the choice between them as
meaningful; in the metered form it is not.

The genuine distinction is with the **queueing** leaky bucket, which is a traffic *shaper*: it admits
the request and delays it until the bucket has drained, producing a perfectly smooth output rate. That
is a different contract — it blocks, it needs a queue, it can reorder — and it does not fit a
non-blocking limiter interface. For evenly-spaced admission without a queue, use GCRA with
`burst = 1`.

## Sliding window counter

**State:** three `long`s. **Exact:** ~1% error. **Bursts:** no.

Keeps the current and previous window counts, and charges the previous one a weight proportional to
how much of it still falls inside the trailing window:

```
elapsed  = now − currentWindowStart
weight   = 1 − elapsed / windowLength
estimate = previousCount × weight + currentCount
```

The approximation assumes the previous window's requests were spread evenly across it. A client that
sent all of its previous-window traffic in that window's first instant is over-charged; one that sent
it all at the end is under-charged. Cloudflare's published figure for the resulting error on
production traffic is well under 1%.

**Use it for large limits where smooth behaviour matters more than exactness.**

### The boundary is the subtle part

The estimate is **continuous** across a window boundary — at the turn, the current count simply
becomes the previous one and starts decaying in its place. Nothing drops.

This is easy to get wrong, and this project got it wrong first. The original `retryAfter` answered
"wait until the window turns over", which is exactly one window; a client that waited precisely that
long came back to find the same estimate it left, and was refused again. The correct answer solves for
the instant the estimate actually decays past the target, which spans two linear segments. The
cross-algorithm contract suite caught it.

## Sliding window log

**State:** `8N` bytes. **Exact:** yes. **Bursts:** no.

Stores the timestamp of every permit and counts those inside the trailing window. "At most N in any
window of length W" holds precisely — no boundary spike, no statistical error.

O(N) memory per key and O(N) work per request. A 10,000/hour policy over a million keys is 80 GB of
timestamps, so this is not a general-purpose limiter.

**Use it for small, high-value limits:** five failed logins per hour, three password resets per day,
one account creation per IP per minute. There N is single digits, the key space is small, and being
exactly right matters. Approximating a login limiter is how "five attempts per hour" quietly becomes
nine.

## Fixed window

**State:** two `long`s. **Exact:** admits up to 2× at a boundary. **Bursts:** n/a.

A counter per key, reset every window. The simplest thing that works, and included mostly so the
others have a baseline.

### The boundary spike is 2× and it is real

With a limit of 100/minute, a client sending 100 requests at 11:59:59 and 100 more at 12:00:01 has
sent 200 requests in two seconds and broken no rule.

Whether that matters depends on *why* the limit exists. For a billing quota it is fine. For protecting
a downstream service whose real constraint is instantaneous concurrency, it is precisely the failure
the limiter was installed to prevent.

**Use it when the window boundary is externally meaningful** — a calendar-month quota, where resetting
on the boundary is the point.

---

## Tiering

Real quotas are almost always tiered: a per-second limit protects the service from a stampede, and a
per-hour limit stops a client consuming a month of quota in an afternoon. Neither alone does both jobs.

`TieredRateLimiter` enforces several policies at once. The implementation detail worth knowing is that
it does **not** simply call `tryAcquire` on each tier: if the per-second tier allows and the per-hour
tier refuses, the request was rejected but the per-second tier has already spent a permit on it. A
client sitting against its hourly limit would silently burn its per-second budget on requests that
never ran.

So it peeks every tier first and commits only if all would allow. That is not atomic across tiers — a
concurrent caller can change the answer in between, so a tier can be pushed slightly past its limit
under contention, bounded by the number of racing threads. Closing that gap needs a lock spanning all
tiers on the hot path of every request, which would make the limiter the bottleneck it exists to
prevent. Where the overshoot is genuinely unacceptable — a hard billing ceiling — enforce that tier
alone, where a single limiter's own atomicity is the guarantee.

## Distributed enforcement

An in-process limiter on N nodes enforces N× the intended limit, and the effective limit changes
whenever the fleet scales. Any limit that is a real contract has to be shared.

`ratelimit4j-redis` runs each decision as one Lua script. Redis executes scripts on a single thread
with nothing interleaved, so read-modify-write is atomic across the fleet with no lock, no
`WATCH`/`MULTI` retry loop, and one round trip.

Scripts read Redis's own clock via `TIME` rather than accepting a caller timestamp. Redis is then the
single authoritative clock for the fleet, and the limit no longer depends on how well the nodes'
clocks agree — with client timestamps, the effective limit becomes a function of the worst clock skew
among them.

Not every algorithm is offered: the sliding window log would need the whole timestamp set shipped or
manipulated per request, and the exactness it buys does not survive being spread over a network.

See [`FailureMode`](../ratelimit4j-redis/src/main/java/io/ratelimit4j/redis/FailureMode.java) for what
happens when Redis is unreachable — a decision the README covers in full, and one with no safe
default.
