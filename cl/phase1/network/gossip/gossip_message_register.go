package gossip

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	serviceintf "github.com/erigontech/erigon/cl/phase1/network/services/service_interface"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

func RegisterGossipService[T any](gm *GossipManager, service serviceintf.Service[T], conditions ...ConditionFunc) (subscribed, expired int) {
	wrappedService := wrapService(service)
	gossipSrv := GossipService{
		Service:    wrappedService,
		conditions: conditions,
	}
	gm.registeredServices = append(gm.registeredServices, gossipSrv)
	subscribed, expired, err := gm.registerGossipService(wrappedService, conditions...)
	if err != nil {
		// A canceled context means the node is shutting down (e.g. the backend's ctx was cancelled mid-startup).
		// RunCaplinService's caller already handles a context.Canceled return gracefully (backend.go), so exit
		// quietly here rather than panicking — a panic in this goroutine would crash the WHOLE process (in a
		// multi-chain node, every chain), turning one chain's shutdown into a total crash.
		if errors.Is(err, context.Canceled) {
			return
		}
		panic(err)
	}
	return
}

type ConditionFunc func(peer.ID, *pubsub.Message, clparams.StateVersion) bool

type GossipService struct {
	Service    serviceintf.Service[any]
	conditions []ConditionFunc
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

func (w *serviceWrapper[T]) DecodeGossipMessage(pid peer.ID, data []byte, version clparams.StateVersion) (any, error) {
	return w.service.DecodeGossipMessage(pid, data, version)
}

func (w *serviceWrapper[T]) ProcessMessage(ctx context.Context, subnet *uint64, msg any) error {
	if typedMsg, ok := msg.(T); ok {
		return w.service.ProcessMessage(ctx, subnet, typedMsg)
	}
	return fmt.Errorf("unexpected message type: %T", msg)
}
