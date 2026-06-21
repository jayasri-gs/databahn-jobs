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

const (
	IncrWithTtl = "IncrWithTtl"
)

var scripHashes = make(map[string]string)

type Scripts struct {
	cli *Client
}

func NewScripts(cli *Client) *Scripts {
	s := Scripts{cli: cli}
	return &s
}

func (scs *Scripts) RegisterScripts(scripts map[string]string) error {
	ctx := context.Background()
	if scripts == nil {
		scripts = make(map[string]string)
	}
	scripts[IncrWithTtl] = incrWithTtl
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
