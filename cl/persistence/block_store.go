package persistence

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
)

var _ BlockSource = (*BeaconRpcSource)(nil)

type BeaconRpcSource struct {
	rpc *rpc.BeaconRpcP2P
}

func (b *BeaconRpcSource) SaveBlocks(ctx context.Context, blocks *peers.PeeredObject[*cltypes.SignedBeaconBlock]) error {
	// it is a no-op because there is no need to do this
	return nil
}

func NewBeaconRpcSource(rpc *rpc.BeaconRpcP2P) *BeaconRpcSource {
	return &BeaconRpcSource{
		rpc: rpc,
	}
}

func (*BeaconRpcSource) GetBlock(ctx context.Context, tx kv.Tx, slot uint64) (*peers.PeeredObject[*cltypes.SignedBeaconBlock], error) {
	panic("unimplemented")
}

func (b *BeaconRpcSource) GetRange(ctx context.Context, _ kv.Tx, from uint64, count uint64) (*peers.PeeredObject[[]*cltypes.SignedBeaconBlock], error) {
	if count == 0 {
		return nil, nil
	}
	var responses *peers.PeeredObject[[]*cltypes.SignedBeaconBlock]
	reqInterval := time.NewTicker(200 * time.Millisecond)
	doneRespCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1)
	defer reqInterval.Stop()

	for {
		select {
		case <-reqInterval.C:
			go func() {
				responses, pid, err := b.rpc.SendBeaconBlocksByRangeReq(ctx, from, count)
				if err != nil {
					return
				}
				select {
				case doneRespCh <- &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{Data: responses, Peer: pid}:
				default:
				}
			}()
		case <-ctx.Done():
			return nil, ctx.Err()
		case responses = <-doneRespCh:
			return responses, nil
		}
	}
}

// a noop for rpc source since we always return new data
func (b *BeaconRpcSource) PurgeRange(ctx context.Context, _ kv.Tx, from uint64, count uint64) error {
	return nil
}
