package connections

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// DAO is the data-access contract the connections Service depends on.
// At present it only exposes the inter-replica publish path used by
// the server to fan out a freshly-ingested message to whichever
// replica owns the recipient's WebSocket (spec §6.2 D1, §6.3).
type DAO interface {
	Publish(ctx context.Context, channel string, payload []byte) error
}

// RedisDAO is the go-redis implementation of DAO.
type RedisDAO struct {
	client *redis.Client
}

// NewRedisDAO wraps an open *redis.Client as a connections DAO.
func NewRedisDAO(client *redis.Client) *RedisDAO {
	return &RedisDAO{client: client}
}

// Publish forwards payload onto the named Redis Pub/Sub channel.
func (r *RedisDAO) Publish(ctx context.Context, channel string, payload []byte) error {
	return r.client.Publish(ctx, channel, payload).Err()
}
