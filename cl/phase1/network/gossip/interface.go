package gossip

import (
	"context"
	"time"
)

//go:generate mockgen -destination=./mock_services/gossip_mock.go -package=mock_services . Gossip
type Gossip interface {
	Publish(ctx context.Context, name string, data []byte) error
	SubscribeWithExpiry(name string, expiry time.Time) error
}
