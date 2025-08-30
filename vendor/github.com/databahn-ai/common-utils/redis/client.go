package redis

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/databahn-ai/go-logging/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// PoolConfig holds Redis connection pool configuration
type PoolConfig struct {
	PoolSize     int           // Maximum number of socket connections
	MinIdleConns int           // Minimum number of idle connections
	MaxConnAge   time.Duration // Connection age at which client retires the connection
	PoolTimeout  time.Duration // Time client waits for connection if all are busy
	IdleTimeout  time.Duration // Time after which idle connections are closed
}

type Client struct {
	redisUrl    string
	serviceName string
	cli         *redis.Client
	poolConfig  *PoolConfig
}

// NewClient creates a Redis client with minimal pooling (backward compatible)
func NewClient(redisUrl, serviceName string) (*Client, error) {
	ctx := context.Background()
	cli := getCli(redisUrl, serviceName)
	res := cli.Ping(ctx)
	err := res.Err()
	if err != nil {
		logger.GetLogger().Error("failed to connect to redis", zap.Error(err), zap.String("url", redisUrl))
		return nil, err
	}
	logger.GetLogger().Info("connected to redis", zap.String("url", redisUrl))
	client := Client{
		redisUrl:    redisUrl,
		serviceName: serviceName,
		cli:         cli,
	}
	return &client, nil
}

// NewClientWithPool creates a Redis client with explicit pool configuration
func NewClientWithPool(redisUrl, serviceName string, poolConfig *PoolConfig) (*Client, error) {
	if poolConfig == nil {
		return nil, errors.New("poolConfig cannot be nil - use NewClient for basic Redis client without pool configuration")
	}

	ctx := context.Background()
	cli := getCliWithPool(redisUrl, serviceName, poolConfig)
	res := cli.Ping(ctx)
	err := res.Err()
	if err != nil {
		logger.GetLogger().Error("failed to connect to redis", zap.Error(err), zap.String("url", redisUrl))
		return nil, err
	}
	logger.GetLogger().Info("connected to redis with pool",
		zap.String("url", redisUrl),
		zap.Int("poolSize", poolConfig.PoolSize),
		zap.Int("minIdleConns", poolConfig.MinIdleConns))
	client := Client{
		redisUrl:    redisUrl,
		serviceName: serviceName,
		cli:         cli,
		poolConfig:  poolConfig,
	}
	return &client, nil
}

func getCli(url, serviceName string) *redis.Client {
	cli := redis.NewClient(&redis.Options{
		Addr:       url,
		ClientName: serviceName,
	})
	return cli
}

// getCliWithPool creates a Redis client with explicit pool configuration
func getCliWithPool(url, serviceName string, poolConfig *PoolConfig) *redis.Client {
	if poolConfig == nil {
		// This should not happen if NewClientWithPool properly validates, but adding for safety
		logger.GetLogger().Error("poolConfig is nil in getCliWithPool, falling back to basic client")
		return getCli(url, serviceName)
	}

	cli := redis.NewClient(&redis.Options{
		Addr:            url,
		ClientName:      serviceName,
		PoolSize:        poolConfig.PoolSize,
		MinIdleConns:    poolConfig.MinIdleConns,
		ConnMaxLifetime: poolConfig.MaxConnAge,
		PoolTimeout:     poolConfig.PoolTimeout,
		ConnMaxIdleTime: poolConfig.IdleTimeout,
	})
	return cli
}

func (c *Client) Ping(ctx context.Context) error {
	res := c.cli.Ping(ctx)
	return res.Err()
}

func (c *Client) Close() error {
	return c.cli.Close()
}

// GetPoolStats returns connection pool statistics
func (c *Client) GetPoolStats() *redis.PoolStats {
	return c.cli.PoolStats()
}

// IsPooled returns whether the client is using explicit pool configuration
func (c *Client) IsPooled() bool {
	return c.poolConfig != nil
}

// GetPoolConfig returns the current pool configuration
func (c *Client) GetPoolConfig() *PoolConfig {
	return c.poolConfig
}

func (c *Client) reload(ctx context.Context) error {
	var cli *redis.Client
	var err error

	if c.poolConfig != nil {
		cli, err = c.TestConnectionWithPool(ctx)
	} else {
		cli, err = c.TestConnection(ctx)
	}

	if err == nil {
		c.cli = cli
	}
	return err
}

func (c *Client) TestConnection(ctx context.Context) (*redis.Client, error) {
	cli := getCli(c.redisUrl, c.serviceName)
	res := cli.Ping(ctx)
	return cli, res.Err()
}

func (c *Client) TestConnectionWithPool(ctx context.Context) (*redis.Client, error) {
	cli := getCliWithPool(c.redisUrl, c.serviceName, c.poolConfig)
	res := cli.Ping(ctx)
	return cli, res.Err()
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	cmd := c.cli.Get(ctx, key)
	return cmd.Result()
}

func (c *Client) Ttl(ctx context.Context, key string) (time.Duration, error) {
	cmd := c.cli.TTL(ctx, key)
	return cmd.Result()
}

func (c *Client) MGet(ctx context.Context, keys ...string) ([]any, error) {
	cmd := c.cli.MGet(ctx, keys...)
	res, err := cmd.Result()
	return res, err
}

func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	cmd := c.cli.Incr(ctx, key)
	return cmd.Result()
}

func (c *Client) Set(ctx context.Context, key string, val any, ttl time.Duration) error {
	cmd := c.cli.Set(ctx, key, val, ttl)
	_, err := cmd.Result()
	return err
}

func (c *Client) SetNx(ctx context.Context, key string, val any, ttl time.Duration) (bool, error) {
	cmd := c.cli.SetNX(ctx, key, val, ttl)
	b, err := cmd.Result()
	return b, err
}

func (c *Client) ValuesInSet(ctx context.Context, setKey string) ([]string, error) {
	cmd := c.cli.SMembers(ctx, setKey)
	result, err := cmd.Result()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	cmd := c.cli.Exists(ctx, key)
	res, err := cmd.Result()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (c *Client) ExistsInSet(ctx context.Context, setKey string, val any) (bool, error) {
	cmd := c.cli.SIsMember(ctx, setKey, val)
	result, err := cmd.Result()
	if err != nil {
		return false, err
	}
	return result, nil
}

func (c *Client) AddInSet(ctx context.Context, setKey string, val ...any) (int64, error) {
	cmd := c.cli.SAdd(ctx, setKey, val)
	result, err := cmd.Result()
	if err != nil {
		return 0, err
	}
	return result, nil
}

func (c *Client) DeleteKey(ctx context.Context, key string) (int64, error) {
	cmd := c.cli.Del(ctx, key)
	return cmd.Result()
}

func (c *Client) ZAdd(ctx context.Context, key string, score float64, val any) error {
	m := redis.Z{
		Score:  score,
		Member: val,
	}
	cmd := c.cli.ZAdd(ctx, key, m)
	_, err := cmd.Result()
	return err
}

func (c *Client) ZCount(ctx context.Context, key string, min, max float64) (int64, error) {
	mn := strconv.FormatFloat(min, 'f', 2, 64)
	mx := strconv.FormatFloat(max, 'f', 2, 64)
	cmd := c.cli.ZCount(ctx, key, mn, mx)
	return cmd.Result()
}

func (c *Client) IncrWithTtl(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	sh := GetScript(IncrWithTtl)
	res := c.cli.EvalSha(ctx, sh, []string{key}, 1, ttl.Seconds())
	r, err := res.Result()
	if err != nil {
		return 0, err
	}
	cnt, ok := r.(int64)
	if !ok {
		logger.GetLogger().Error("incorrect response type from incr with ttl script", zap.Any("resp", cnt))
		return 0, errors.New("incorrect response type from incr with ttl script")
	}
	return cnt, nil
}

func (c *Client) ZRemoveByScoreRange(ctx context.Context, key string, min, max float64) (int64, error) {
	mn := strconv.FormatFloat(min, 'f', 2, 64)
	mx := strconv.FormatFloat(max, 'f', 2, 64)
	cmd := c.cli.ZRemRangeByScore(ctx, key, mn, mx)
	return cmd.Result()
}

func (c *Client) EvalScript(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	res := c.cli.EvalSha(ctx, script, keys, args...)
	return res.Result()
}
