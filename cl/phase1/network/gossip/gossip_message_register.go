package gossip

import (
	context0 "context"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	serviceintf "github.com/erigontech/erigon/cl/phase1/network/services/service_interface"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

func RegisterGossipService[T any](gm *GossipManager, service serviceintf.Service[T], conditions ...ConditionFunc) {
	wrappedService := wrapService(service)
	gossipSrv := GossipService{
		Service:    wrappedService,
		conditions: conditions,
	}
	gm.registeredServices = append(gm.registeredServices, gossipSrv)
	if err := gm.registerGossipService(wrappedService, conditions...); err != nil {
		panic(err)
	}
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

func (w *serviceWrapper[T]) ProcessMessage(ctx context0.Context, subnet *uint64, msg any) error {
	if typedMsg, ok := msg.(T); ok {
		return w.service.ProcessMessage(ctx, subnet, typedMsg)
	}
	return fmt.Errorf("unexpected message type: %T", msg)
}
