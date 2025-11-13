package network

import (
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/net/context"
)

type conditionFunc func(peer.ID, *pubsub.Message, clparams.StateVersion) bool

type gossipService struct {
	service    services.Service[any]
	conditions []conditionFunc
}

func (s *gossipService) SatisfiesConditions(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
	for _, condition := range s.conditions {
		if !condition(pid, msg, curVersion) {
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

func (w *serviceWrapper[T]) Names() []string {
	return w.service.Names()
}

func (w *serviceWrapper[T]) IsMyGossipMessage(name string) bool {
	return w.service.IsMyGossipMessage(name)
}

func (w *serviceWrapper[T]) DecodeGossipMessage(pid peer.ID, data []byte, version clparams.StateVersion) (any, error) {
	return w.service.DecodeGossipMessage(pid, data, version)
}

func (w *serviceWrapper[T]) ProcessMessage(ctx context.Context, subnet *uint64, msg any) error {
	if typedMsg, ok := msg.(T); ok {
		return w.service.ProcessMessage(ctx, subnet, typedMsg)
	}
	return fmt.Errorf("unexpected message type: %T", msg)
}

func RegisterGossipService[T any](gm *GossipManager, service services.Service[T], conditions ...conditionFunc) {

	wrappedService := wrapService(service)
	gossipSrv := gossipService{
		service:    wrappedService,
		conditions: conditions,
	}
	gm.registeredServices = append(gm.registeredServices, gossipSrv)
	gm.registerGossipService(gossipSrv)
}

// withBeginVersion returns a condition that checks if the current version is greater than or equal to the begin version
func withBeginVersion(beginVersion clparams.StateVersion) conditionFunc {
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return curVersion >= beginVersion
	}
}

// withEndVersion returns a condition that checks if the current version is less than the end version
func withEndVersion(endVersion clparams.StateVersion) conditionFunc {
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return curVersion < endVersion
	}
}

// withGlobalTimeBasedRateLimiter returns a condition that checks if the message can be processed based on the time based rate limiter
func withGlobalTimeBasedRateLimiter(duration time.Duration, maxRequests int) conditionFunc {
	limiter := newTimeBasedRateLimiter(duration, maxRequests)
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return limiter.tryAcquire()
	}
}

// withRateLimiterByPeer returns a condition that checks if the message can be processed based on the token bucket rate limiter
func withRateLimiterByPeer(ratePerSecond float64, burst int) conditionFunc {
	limiter := newTokenBucketRateLimiter(ratePerSecond, burst)
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return limiter.acquire(pid.String())
	}
}
