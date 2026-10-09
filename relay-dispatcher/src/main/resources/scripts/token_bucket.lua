-- Token bucket, evaluated atomically by Redis. One bucket per endpoint, shared by every dispatcher instance.
-- KEYS[1]  bucket key
-- ARGV[1]  refill rate, tokens per second
-- ARGV[2]  capacity (burst)
-- Returns 0 if a token was taken, otherwise the milliseconds until one will be available.
-- Uses the Redis server clock, so dispatcher clock skew cannot mint extra tokens.
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end

tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate / 1000)

local wait = 0
if tokens >= 1 then
  tokens = tokens - 1
else
  wait = math.ceil((1 - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity * 1000 / rate) + 1000)
return wait
