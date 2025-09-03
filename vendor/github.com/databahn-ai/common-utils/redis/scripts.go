package redis

import (
	"context"
	"crypto/sha1"
	"encoding/hex"

	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

const incrWithTtl = `
local key = KEYS[1]
local increment = ARGV[1]
local ttl = tonumber(ARGV[2])
local current = tonumber(redis.call("GET", key))
if current then
    redis.call("INCRBY", key, increment)
else
    redis.call("SET", key, increment)
    if ttl > 0 then
        redis.call("EXPIRE", key, ttl)
    end
end
return tonumber(redis.call("GET", key))
`

const suppressionScript = `
local rule = KEYS[1]
local key = KEYS[2]

local ttl = tonumber(ARGV[1])
local maxKeys = tonumber(ARGV[2])
local ttlMin = tonumber(ARGV[3])
local ttlMax = tonumber(ARGV[4])

local inc = redis.call("INCR", key)
if inc == 1 then
    if ttl > 0 then
        redis.call("EXPIRE", key, ttl)
    end
	local window = redis.call("ZCOUNT", rule, ttlMin, ttlMax)
    if window and tonumber(window) >= maxKeys then
        redis.call("DEL", key)
        return -1
    else
        redis.call("ZADD", rule, ttlMax, key)
    end
end
return inc
`

const (
	IncrWithTtl       = "IncrWithTtl"
	SuppressionScript = "SuppressionScript"
)

var scripHashes = make(map[string]string)

type Scripts struct {
	cli *Client
}

func NewScripts(cli *Client) *Scripts {
	s := Scripts{cli: cli}
	return &s
}

func (scs *Scripts) RegisterScripts() error {
	ctx := context.Background()
	scripts := make(map[string]string)
	scripts[IncrWithTtl] = incrWithTtl
	scripts[SuppressionScript] = suppressionScript
	for nm, sc := range scripts {
		sh := sha1.New()
		sh.Write([]byte(sc))
		hash := sh.Sum(nil)
		shhx := hex.EncodeToString(hash)
		exRes, err := scs.cli.cli.ScriptExists(ctx, shhx).Result()
		if err != nil {
			return err
		}
		if !exRes[0] {
			shNew, err := scs.cli.cli.ScriptLoad(ctx, sc).Result()
			if err != nil {
				return err
			}
			logger.GetLogger().Info("registered new script", zap.String("name", nm), zap.String("sha", shNew))
			if shhx != shNew {
				logger.GetLogger().Error("sh1 mismatch for script", zap.String("name", nm))
				shhx = shNew
			}
		}
		scripHashes[nm] = shhx
	}
	return nil
}

func GetScript(script string) string {
	return scripHashes[script]
}
