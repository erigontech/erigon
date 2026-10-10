package gossip

import (
	"context"
	"time"

	"github.com/erigontech/erigon/common"
)

//go:generate mockgen -destination=./mock_services/gossip_mock.go -package=mock_services . Gossip
type Gossip interface {
	Publish(ctx context.Context, name string, data []byte) error
	PublishToForkDigest(ctx context.Context, forkDigest common.Bytes4, name string, data []byte) error
	// PublishBackground queues data for asynchronous publish to the given
	// gossip topic without waiting for the network call. The implementation
	// clones data before returning, so the caller keeps ownership of its
	// own slice and may reuse or mutate it immediately. It never blocks -
	// not on queue capacity, and not on shutdown - though it does resolve
	// the fork digest and log synchronously. A non-nil return means the
	// message was never admitted to the queue
	// (full, shut down, already expired, or the fork digest could not be
	// resolved) - the caller knows this before it responds and should
	// surface it, rather than the eventual network outcome, which this
	// call never reports. expiry, if non-zero, is the latest time the
	// message is still worth publishing; PublishBackground checks it both
	// at admission and again just before the actual publish.
	PublishBackground(name string, data []byte, expiry time.Time, logCtx ...any) error
	SubscribeWithExpiry(name string, expiry time.Time) error
}
