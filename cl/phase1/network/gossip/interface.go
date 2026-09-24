package gossip

import (
	"context"
	"time"
)

//go:generate mockgen -destination=./mock_services/gossip_mock.go -package=mock_services . Gossip
type Gossip interface {
	Publish(ctx context.Context, name string, data []byte) error
	// PublishBackground queues data for asynchronous publish to the given
	// gossip topic without waiting for the network call. It does not block
	// on queue capacity or shutdown, but does take a lock, resolve the fork
	// digest, and log synchronously. A non-nil return means the message was
	// never admitted to the queue (full, shut down, already expired, or the
	// fork digest could not be resolved) - the caller knows this before it
	// responds and should surface it, rather than the eventual network
	// outcome, which this call never reports. expiry, if non-zero, is the
	// latest time the message is still worth publishing; PublishBackground
	// checks it both at admission and again just before the actual publish.
	PublishBackground(name string, data []byte, expiry time.Time, logCtx ...any) error
	SubscribeWithExpiry(name string, expiry time.Time) error
}
