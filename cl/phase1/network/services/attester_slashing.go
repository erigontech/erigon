package services

import (
	"context"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/libp2p/go-libp2p/core/peer"
)

type attesterSlashingService struct {
	forkchoiceStore *forkchoice.ForkChoiceStore
}

func NewAttesterSlashingService(
	forkchoiceStore *forkchoice.ForkChoiceStore,
) AttesterSlashingService {
	return &attesterSlashingService{
		forkchoiceStore: forkchoiceStore,
	}
}

func (s *attesterSlashingService) Names() []string {
	return []string{gossip.TopicNameAttesterSlashing}
}

func (s *attesterSlashingService) IsMyGossipMessage(name string) bool {
	return name == gossip.TopicNameAttesterSlashing
}

func (s *attesterSlashingService) DecodeGossipMessage(_ peer.ID, data []byte, version clparams.StateVersion) (*cltypes.AttesterSlashing, error) {
	obj := &cltypes.AttesterSlashing{}
	if err := obj.DecodeSSZ(data, int(version)); err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *attesterSlashingService) ProcessMessage(ctx context.Context, subnet *uint64, msg *cltypes.AttesterSlashing) error {
	return s.forkchoiceStore.OnAttesterSlashing(msg, false)
}
