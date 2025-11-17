package gossip

import (
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	serviceintf "github.com/erigontech/erigon/cl/phase1/network/services/service_interface"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/net/context"
)

func RegisterGossipService[T any](gm *GossipManager, service serviceintf.Service[T], conditions ...conditionFunc) {
	wrappedService := wrapService(service)
	gossipSrv := GossipService{
		Service:    wrappedService,
		conditions: conditions,
	}
	gm.registeredServices = append(gm.registeredServices, gossipSrv)
	if err := gm.registerGossipService(gossipSrv); err != nil {
		panic(err)
	}
}

type conditionFunc func(peer.ID, *pubsub.Message, clparams.StateVersion) bool

type GossipService struct {
	Service    serviceintf.Service[any]
	conditions []conditionFunc
}

func (s *GossipService) SatisfiesConditions(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
	for _, condition := range s.conditions {
		if !condition(pid, msg, curVersion) {
			return false
		}
	}
	return true
}

// wrapService wraps a service to return an any type service
func wrapService[T any](service serviceintf.Service[T]) serviceintf.Service[any] {
	return &serviceWrapper[T]{service: service}
}

type serviceWrapper[T any] struct {
	service serviceintf.Service[T]
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

// WithBeginVersion returns a condition that checks if the current version is greater than or equal to the begin version
func WithBeginVersion(beginVersion clparams.StateVersion) conditionFunc {
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return curVersion >= beginVersion
	}
}

// WithEndVersion returns a condition that checks if the current version is less than the end version
func WithEndVersion(endVersion clparams.StateVersion) conditionFunc {
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return curVersion < endVersion
	}
}

// WithGlobalTimeBasedRateLimiter returns a condition that checks if the message can be processed based on the time based rate limiter
func WithGlobalTimeBasedRateLimiter(duration time.Duration, maxRequests int) conditionFunc {
	limiter := newTimeBasedRateLimiter(duration, maxRequests)
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return limiter.tryAcquire()
	}
}

// WithRateLimiterByPeer returns a condition that checks if the message can be processed based on the token bucket rate limiter
func WithRateLimiterByPeer(ratePerSecond float64, burst int) conditionFunc {
	limiter := newTokenBucketRateLimiter(ratePerSecond, burst)
	return func(pid peer.ID, msg *pubsub.Message, curVersion clparams.StateVersion) bool {
		return limiter.acquire(pid.String())
	}
}
