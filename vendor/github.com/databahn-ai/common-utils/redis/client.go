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

func (c *Client) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	res := c.cli.Eval(ctx, script, keys, args...)
	return res.Result()
}

// Hash operations (HSET family)

// HSet sets field in the hash stored at key to value
func (c *Client) HSet(ctx context.Context, key, field string, value any) error {
	cmd := c.cli.HSet(ctx, key, field, value)
	_, err := cmd.Result()
	return err
}

// HGet returns the value associated with field in the hash stored at key
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	cmd := c.cli.HGet(ctx, key, field)
	return cmd.Result()
}

// HMSet sets multiple field-value pairs in the hash stored at key
func (c *Client) HMSet(ctx context.Context, key string, values map[string]any) error {
	cmd := c.cli.HMSet(ctx, key, values)
	_, err := cmd.Result()
	return err
}

// HMGet returns the values associated with the specified fields in the hash stored at key
func (c *Client) HMGet(ctx context.Context, key string, fields ...string) ([]any, error) {
	cmd := c.cli.HMGet(ctx, key, fields...)
	return cmd.Result()
}

// HGetAll returns all fields and values of the hash stored at key
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	cmd := c.cli.HGetAll(ctx, key)
	return cmd.Result()
}

// HExists returns whether field exists in the hash stored at key
func (c *Client) HExists(ctx context.Context, key, field string) (bool, error) {
	cmd := c.cli.HExists(ctx, key, field)
	return cmd.Result()
}

// HDel removes the specified fields from the hash stored at key
func (c *Client) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	cmd := c.cli.HDel(ctx, key, fields...)
	return cmd.Result()
}

// HLen returns the number of fields in the hash stored at key
func (c *Client) HLen(ctx context.Context, key string) (int64, error) {
	cmd := c.cli.HLen(ctx, key)
	return cmd.Result()
}

// HKeys returns all field names in the hash stored at key
func (c *Client) HKeys(ctx context.Context, key string) ([]string, error) {
	cmd := c.cli.HKeys(ctx, key)
	return cmd.Result()
}

// HVals returns all values in the hash stored at key
func (c *Client) HVals(ctx context.Context, key string) ([]string, error) {
	cmd := c.cli.HVals(ctx, key)
	return cmd.Result()
}

// HIncrBy increments the number stored at field in the hash stored at key by increment
func (c *Client) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	cmd := c.cli.HIncrBy(ctx, key, field, incr)
	return cmd.Result()
}

// HIncrByFloat increments the float value stored at field in the hash stored at key by increment
func (c *Client) HIncrByFloat(ctx context.Context, key, field string, incr float64) (float64, error) {
	cmd := c.cli.HIncrByFloat(ctx, key, field, incr)
	return cmd.Result()
}

// HSetNX sets field in the hash stored at key to value, only if field does not yet exist
func (c *Client) HSetNX(ctx context.Context, key, field string, value any) (bool, error) {
	cmd := c.cli.HSetNX(ctx, key, field, value)
	return cmd.Result()
}

// LRange returns the specified elements of the list stored at key
func (c *Client) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	cmd := c.cli.LRange(ctx, key, start, stop)
	return cmd.Result()
}

// LIndex returns the element at index in the list stored at key
func (c *Client) LIndex(ctx context.Context, key string, index int64) (string, error) {
	cmd := c.cli.LIndex(ctx, key, index)
	return cmd.Result()
}
