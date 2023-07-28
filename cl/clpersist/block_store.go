package clpersist

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/tidwall/btree"
)

type BlockSource interface {
	GetRange(ctx context.Context, from uint64, count uint64) ([]*cltypes.SignedBeaconBlock, error)
	PurgeRange(ctx context.Context, from uint64, count uint64) error
}

type LayeredBeaconSource struct {
}

var _ BlockSource = (*BeaconRpcSource)(nil)

type BeaconRpcSource struct {
	rpc *rpc.BeaconRpcP2P
}

func NewBeaconRpcSource(rpc *rpc.BeaconRpcP2P) *BeaconRpcSource {
	return nil
}

func (b *BeaconRpcSource) GetRange(ctx context.Context, from uint64, count uint64) ([]*cltypes.SignedBeaconBlock, error) {
	if count == 0 {
		return nil, nil
	}
	responses, pid, err := b.rpc.SendBeaconBlocksByRangeReq(ctx, from, from+count)
	if err != nil {
		b.rpc.BanPeer(pid)
		// Wait a bit in this case (we do not need to be super performant here).
		return nil, err
	}
	return responses, nil
}

// a noop for rpc source since we always return new data
func (b *BeaconRpcSource) PurgeRange(ctx context.Context, from uint64, count uint64) error {
	return nil
}

var _ BlockSource = (*CachingSource)(nil)

type CachingSource struct {
	parent BlockSource

	blocks *btree.Map[uint64, *cltypes.SignedBeaconBlock]
	mu     sync.Mutex
}

func NewCachingSource(parent BlockSource) *CachingSource {
	return &CachingSource{
		parent: parent,
		blocks: btree.NewMap[uint64, *cltypes.SignedBeaconBlock](32),
	}
}

func (b *CachingSource) GetRange(ctx context.Context, from uint64, count uint64) ([]*cltypes.SignedBeaconBlock, error) {
	responses, err := b.parent.GetRange(ctx, from, count)
	if err != nil {
		return nil, err
	}
	out := make([]*cltypes.SignedBeaconBlock, 0, count)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, v := range responses {
		b.blocks.Set(v.Block.Slot, v)
	}
	b.blocks.Ascend(from, func(key uint64, value *cltypes.SignedBeaconBlock) bool {
		if len(out) >= int(count) {
			return false
		}
		out = append(out, value)
		return true
	})
	return out, err
}

func (b *CachingSource) PurgeRange(ctx context.Context, from uint64, count uint64) error {
	b.parent.PurgeRange(ctx, from, count)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocks.AscendMut(from, func(key uint64, value *cltypes.SignedBeaconBlock) bool {
		if key >= from+count {
			return false
		}
		b.blocks.Delete(key)
		return true
	})
	return nil
}
