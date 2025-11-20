package network

import (
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"golang.org/x/net/context"
)

type gossipService struct {
	service    services.Service[any]
	conditions []func(*sentinelproto.GossipData, clparams.StateVersion) bool
}

func (s *gossipService) SatisfiesConditions(data *sentinelproto.GossipData, curVersion clparams.StateVersion) bool {
	for _, condition := range s.conditions {
		if !condition(data, curVersion) {
			return false
		}
	}
	return true
}

// wrapService wraps a service to return an any type service
func wrapService[T any](service services.Service[T]) services.Service[any] {
	return &serviceWrapper[T]{service: service}
}

type serviceWrapper[T any] struct {
	service services.Service[T]
}

func (w *serviceWrapper[T]) IsMyGossipMessage(name string) bool {
	return w.service.IsMyGossipMessage(name)
}

func (w *serviceWrapper[T]) DecodeGossipMessage(data *sentinelproto.GossipData, version clparams.StateVersion) (any, error) {
	return w.service.DecodeGossipMessage(data, version)
}

func (w *serviceWrapper[T]) ProcessMessage(ctx context.Context, subnet *uint64, msg any) error {
	if typedMsg, ok := msg.(T); ok {
		return w.service.ProcessMessage(ctx, subnet, typedMsg)
	}
	return fmt.Errorf("unexpected message type: %T", msg)
}

func RegisterGossipService[T any](gm *GossipManager, service services.Service[T], conditions ...func(data *sentinelproto.GossipData, curVersion clparams.StateVersion) bool) {
	gm.registeredServices = append(gm.registeredServices, gossipService{
		service:    wrapService(service),
		conditions: conditions,
	})
}

// withBeginVersion returns a condition that checks if the current version is greater than or equal to the begin version
func withBeginVersion(beginVersion clparams.StateVersion) func(_ *sentinelproto.GossipData, curVersion clparams.StateVersion) bool {
	return func(_ *sentinelproto.GossipData, curVersion clparams.StateVersion) bool {
		return curVersion >= beginVersion
	}
}

// withEndVersion returns a condition that checks if the current version is less than the end version
func withEndVersion(endVersion clparams.StateVersion) func(_ *sentinelproto.GossipData, curVersion clparams.StateVersion) bool {
	return func(_ *sentinelproto.GossipData, curVersion clparams.StateVersion) bool {
		return curVersion < endVersion
	}
}

// withGlobalTimeBasedRateLimiter returns a condition that checks if the message can be processed based on the time based rate limiter
func withGlobalTimeBasedRateLimiter(duration time.Duration, maxRequests int) func(_ *sentinelproto.GossipData, curVersion clparams.StateVersion) bool {
	limiter := newTimeBasedRateLimiter(duration, maxRequests)
	return func(_ *sentinelproto.GossipData, _ clparams.StateVersion) bool {
		return limiter.tryAcquire()
	}
}

// withRateLimiterByPeer returns a condition that checks if the message can be processed based on the token bucket rate limiter
func withRateLimiterByPeer(ratePerSecond float64, burst int) func(_ *sentinelproto.GossipData, _ clparams.StateVersion) bool {
	limiter := newTokenBucketRateLimiter(ratePerSecond, burst)
	return func(data *sentinelproto.GossipData, _ clparams.StateVersion) bool {
		return limiter.acquire(data.Peer.Pid)
	}
}
