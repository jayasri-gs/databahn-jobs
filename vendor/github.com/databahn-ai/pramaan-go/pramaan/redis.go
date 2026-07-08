package pramaan

import (
	"context"
	"fmt"

	dbredis "github.com/databahn-ai/common-utils/redis"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/network"
)

/*
RedisPramaan is a utility to start Redis for module testing.
*/
type RedisPramaan struct {
	t             TestLogger
	Container     *tcredis.RedisContainer
	dbRedisClient *dbredis.Client
	redisUrl      string
}

const (
	redisInternalNetworkDns = "pramaanredis"
)

func NewRedisPramaan(ctx context.Context, t TestLogger, dockerNetwork *testcontainers.DockerNetwork) *RedisPramaan {
	redisContainer, err := tcredis.Run(ctx,
		"redis:7",
		tcredis.WithLogLevel(tcredis.LogLevelVerbose),
		network.WithNetwork([]string{redisInternalNetworkDns}, dockerNetwork),
	)
	if err != nil {
		t.Fatalf("failed to create redis container : %v", err)
	}
	redisHost, err := redisContainer.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get host : %v", err)
	}
	port, err := redisContainer.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("failed to get port: %v", err)
	}

	redisUrlForExternalAccess := fmt.Sprintf("%s:%d", redisHost, port.Num())

	redisUrlForInternalAccess := fmt.Sprintf("%s:6379", redisInternalNetworkDns)

	dbRedisClient, err := dbredis.NewClient(redisUrlForExternalAccess, "Pramaan-Go", false)
	if err != nil {
		t.Fatalf("failed to get wrappered redis client that is DbRedisClient : %v", err)
	}

	return &RedisPramaan{
		t:             t,
		Container:     redisContainer,
		dbRedisClient: dbRedisClient,
		redisUrl:      redisUrlForInternalAccess,
	}
}

func (r *RedisPramaan) GetRedisClient() *dbredis.Client {
	return r.dbRedisClient
}
