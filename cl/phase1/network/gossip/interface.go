package gossip

import (
	"context"
	"time"
)

//go:generate mockgen -destination=./mock_services/gossip_mock.go -package=mock_services . Gossip
type Gossip interface {
	Publish(ctx context.Context, name string, data []byte) error
	// PublishBackground queues data for asynchronous publish to the given gossip
	// topic without blocking the caller. It never returns a publish error; a
	// failure is logged by the implementation instead.
	PublishBackground(name string, data []byte, logCtx ...any)
	SubscribeWithExpiry(name string, expiry time.Time) error
}
