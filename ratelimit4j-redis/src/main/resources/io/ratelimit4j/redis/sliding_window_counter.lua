-- Sliding window counter, executed atomically inside Redis.
--
-- Keeps the current and previous window counts in one hash and charges the previous window a weight
-- proportional to how much of it still falls inside the trailing window. See
-- SlidingWindowCounterRateLimiter for the derivation and the accuracy trade.
--
-- KEYS[1]   the limiter key
-- ARGV[1]   window length in microseconds
-- ARGV[2]   permit budget per window
-- ARGV[3]   permits requested
-- ARGV[4]   key TTL in milliseconds
-- ARGV[5]   1 to consume on success, 0 to peek
--
-- Returns {allowed, remaining, retryAfterMicros}

local window  = tonumber(ARGV[1])
local limit   = tonumber(ARGV[2])
local permits = tonumber(ARGV[3])
local ttl     = tonumber(ARGV[4])
local consume = tonumber(ARGV[5]) == 1

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])

local state = redis.call('HMGET', KEYS[1], 'start', 'prev', 'curr')
local start = tonumber(state[1])
local prev  = tonumber(state[2])
local curr  = tonumber(state[3])

if start == nil then
  start = now
  prev = 0
  curr = 0
end

-- Roll forward to the window containing now. Exactly one window elapsed means the current count
-- becomes the previous one; two or more means everything is outside the trailing window.
local elapsed = now - start
if elapsed >= window then
  if elapsed < 2 * window then
    start = start + window
    prev = curr
    curr = 0
  else
    start = start + (math.floor(elapsed / window) * window)
    prev = 0
    curr = 0
  end
end

local weight = 1 - ((now - start) / window)
if weight < 0 then weight = 0 end
local estimate = (prev * weight) + curr

if estimate + permits > limit then
  redis.call('HSET', KEYS[1], 'start', start, 'prev', prev, 'curr', curr)
  redis.call('PEXPIRE', KEYS[1], ttl)

  -- Time until the estimate decays far enough. The estimate is continuous across the boundary --
  -- the current count simply becomes the previous one -- so "wait for the window to turn over" is
  -- not an answer; both segments have to be considered. See the Java implementation for the full
  -- derivation.
  local target = limit - permits
  local to_boundary = math.max(0, (start + window) - now)
  local wait
  if target >= curr and prev > 0 then
    wait = math.ceil((estimate - target) / (prev / window))
    if wait > to_boundary then wait = to_boundary end
  elseif curr <= 0 then
    wait = to_boundary
  elseif target <= 0 then
    wait = to_boundary + window
  else
    local into_next = math.ceil(window * (1 - (target / curr)))
    if into_next > window then into_next = window end
    if into_next < 0 then into_next = 0 end
    wait = to_boundary + into_next
  end
  return {0, math.max(0, math.floor(limit - estimate)), wait}
end

if consume then
  curr = curr + permits
end

redis.call('HSET', KEYS[1], 'start', start, 'prev', prev, 'curr', curr)
redis.call('PEXPIRE', KEYS[1], ttl)

local after = (prev * weight) + curr
return {1, math.max(0, math.floor(limit - after)), 0}
