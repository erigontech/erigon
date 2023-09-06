package persistence

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
)

type BlockSource interface {
	GetRange(ctx context.Context, from uint64, count uint64) ([]*peers.PeeredObject[*cltypes.SignedBeaconBlock], error)
	PurgeRange(ctx context.Context, from uint64, count uint64) error
}

type BeaconChainWriter interface {
	WriteBlock(ctx context.Context, block *cltypes.SignedBeaconBlock, canonical bool) error
}

type BeaconChainDatabase interface {
	BlockSource
	BeaconChainWriter
}
