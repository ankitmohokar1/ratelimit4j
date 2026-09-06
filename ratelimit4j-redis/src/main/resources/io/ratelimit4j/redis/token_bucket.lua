-- Token bucket, executed atomically inside Redis.
--
-- Two fields (tokens and the last refill instant) held in one hash, so the pair is read and written
-- together and can never be observed half-updated.
--
-- KEYS[1]   the limiter key
-- ARGV[1]   bucket capacity, in permits
-- ARGV[2]   refill rate, permits per microsecond
-- ARGV[3]   permits requested
-- ARGV[4]   key TTL in milliseconds
-- ARGV[5]   1 to consume on success, 0 to peek
--
-- Returns {allowed, remaining, retryAfterMicros}

local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local permits  = tonumber(ARGV[3])
local ttl      = tonumber(ARGV[4])
local consume  = tonumber(ARGV[5]) == 1

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local last   = tonumber(state[2])

if tokens == nil or last == nil then
  tokens = capacity
  last = now
end

-- Lazy refill: what accrued since the previous request, capped at capacity. Nothing ticks in the
-- background, so an idle key costs nothing until it is touched again.
local elapsed = math.max(0, now - last)
tokens = math.min(capacity, tokens + (elapsed * rate))

if tokens < permits then
  local shortfall = permits - tokens
  local wait = math.ceil(shortfall / rate)
  -- The refill is still written back: it is a function of elapsed time alone, so persisting it keeps
  -- the stored state canonical without granting anything.
  redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
  redis.call('PEXPIRE', KEYS[1], ttl)
  return {0, math.floor(tokens), wait}
end

if consume then
  tokens = tokens - permits
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], ttl)
return {1, math.floor(tokens), 0}
