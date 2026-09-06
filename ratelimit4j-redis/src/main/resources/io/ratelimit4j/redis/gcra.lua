-- GCRA (Generic Cell Rate Algorithm), executed atomically inside Redis.
--
-- The entire read-modify-write happens in one script invocation. Redis runs scripts on a single
-- thread with nothing interleaved, so this is atomic across the whole fleet without any lock,
-- transaction, or optimistic retry on the client side. That property is why the algorithms in this
-- library are written as pure state transitions: the transition moves here unchanged, and the
-- concurrency strategy is simply "Redis is single-threaded".
--
-- The state is a single value -- the theoretical arrival time -- which is what makes GCRA the natural
-- choice for the distributed backend. There is no multi-field consistency to preserve, so no HSET/HGET
-- pair and no partial update to reason about.
--
-- KEYS[1]   the limiter key
-- ARGV[1]   emission interval, microseconds per permit
-- ARGV[2]   delay tolerance, microseconds
-- ARGV[3]   permits requested
-- ARGV[4]   key TTL in milliseconds
-- ARGV[5]   1 to consume on success, 0 to peek
--
-- Returns {allowed, remaining, retryAfterMicros}

local interval  = tonumber(ARGV[1])
local tolerance = tonumber(ARGV[2])
local permits   = tonumber(ARGV[3])
local ttl       = tonumber(ARGV[4])
local consume   = tonumber(ARGV[5]) == 1

-- Redis's own clock, not the caller's. Every gateway node sharing this Redis therefore shares one
-- authoritative time source, so the limit holds even when the nodes' own clocks disagree. Passing a
-- client timestamp in would make the effective limit a function of the worst clock skew in the fleet.
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])

local tat = tonumber(redis.call('GET', KEYS[1]))
if tat == nil or tat < now then
  -- Absent or in the past: the bucket is full. Clamping to now stops idle time accruing credit
  -- beyond the burst allowance.
  tat = now
end

local new_tat   = tat + (permits * interval)
local allowed_at = new_tat - tolerance

if allowed_at > now then
  local headroom = (now + tolerance) - tat
  local remaining = math.max(0, math.floor(headroom / interval))
  return {0, remaining, allowed_at - now}
end

if consume then
  -- PSETEX in the same call: the TTL is what reclaims keys for clients that never come back, so it
  -- must be refreshed on every write or a busy key would eventually expire mid-window.
  redis.call('PSETEX', KEYS[1], ttl, new_tat)
  tat = new_tat
end

local headroom = (now + tolerance) - math.max(tat, now)
local remaining = math.max(0, math.floor(headroom / interval))
return {1, remaining, 0}
