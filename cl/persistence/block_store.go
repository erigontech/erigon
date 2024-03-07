package persistence

import (
	"context"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/tidwall/btree"
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

var _ BlockSource = (*GossipSource)(nil)

type GossipSource struct {
	mu     sync.Mutex
	blocks *btree.Map[uint64, []*peers.PeeredObject[*cltypes.SignedBeaconBlock]]
}

func (*GossipSource) GetBlock(ctx context.Context, tx kv.Tx, slot uint64) (*peers.PeeredObject[*cltypes.SignedBeaconBlock], error) {
	panic("unimplemented")
}

func NewGossipSource(ctx context.Context) *GossipSource {
	g := &GossipSource{
		blocks: btree.NewMap[uint64, []*peers.PeeredObject[*cltypes.SignedBeaconBlock]](32),
	}

	return g
}

func (b *GossipSource) InsertBlock(ctx context.Context, block *peers.PeeredObject[*cltypes.SignedBeaconBlock]) {
	b.mu.Lock()
	defer b.mu.Unlock()
	current, ok := b.blocks.Get(block.Data.Block.Slot)
	if !ok {
		current = make([]*peers.PeeredObject[*cltypes.SignedBeaconBlock], 0, 1)
	}
	current = append(current, block)
	b.blocks.Set(block.Data.Block.Slot, current)
}

func (b *GossipSource) GetRange(ctx context.Context, _ kv.Tx, from uint64, count uint64) (*peers.PeeredObject[[]*cltypes.SignedBeaconBlock], error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{}
	for i := from; i < from+count; i++ {
		current, ok := b.blocks.Get(i)
		if !ok {
			continue
		}
		for _, v := range current {
			out.Data = append(out.Data, v.Data)
		}
	}
	return out, nil
}

func (b *GossipSource) PurgeRange(ctx context.Context, tx kv.Tx, from uint64, count uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.purgeRange(ctx, tx, from, count)
}

func (b *GossipSource) purgeRange(ctx context.Context, _ kv.Tx, from uint64, count uint64) error {
	initSize := count
	if initSize > 256 {
		initSize = 256
	}
	xs := make([]uint64, 0, initSize)
	b.blocks.Ascend(from, func(key uint64, value []*peers.PeeredObject[*cltypes.SignedBeaconBlock]) bool {
		if key >= from+count {
			return false
		}
		xs = append(xs, key)
		return true
	})
	for _, v := range xs {
		b.blocks.Delete(v)
	}
	return nil
}
