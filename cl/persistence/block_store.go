package persistence

import (
	"context"
	"database/sql"
	"sync"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/tidwall/btree"
)

var _ BlockSource = (*BeaconRpcSource)(nil)

type BeaconRpcSource struct {
	rpc *rpc.BeaconRpcP2P
}

func (b *BeaconRpcSource) SaveBlocks(ctx context.Context, blocks []*peers.PeeredObject[*cltypes.SignedBeaconBlock]) error {
	// it is a no-op because there is no need to do this
	return nil
}

func NewBeaconRpcSource(rpc *rpc.BeaconRpcP2P) *BeaconRpcSource {
	return &BeaconRpcSource{
		rpc: rpc,
	}
}

func (b *BeaconRpcSource) GetRange(_ *sql.Tx, ctx context.Context, from uint64, count uint64) ([]*peers.PeeredObject[*cltypes.SignedBeaconBlock], error) {
	if count == 0 {
		return nil, nil
	}
	responses, pid, err := b.rpc.SendBeaconBlocksByRangeReq(ctx, from, count)
	if err != nil {
		b.rpc.BanPeer(pid)
		return nil, err
	}
	out := make([]*peers.PeeredObject[*cltypes.SignedBeaconBlock], 0, len(responses))
	for _, v := range responses {
		out = append(out, &peers.PeeredObject[*cltypes.SignedBeaconBlock]{Data: v, Peer: pid})
	}
	return out, nil
}

// a noop for rpc source since we always return new data
func (b *BeaconRpcSource) PurgeRange(_ *sql.Tx, ctx context.Context, from uint64, count uint64) error {
	return nil
}

var _ BlockSource = (*GossipSource)(nil)

type GossipSource struct {
	gossip       *network.GossipManager
	gossipBlocks <-chan *peers.PeeredObject[*cltypes.SignedBeaconBlock]

	mu     sync.Mutex
	blocks *btree.Map[uint64, chan *peers.PeeredObject[*cltypes.SignedBeaconBlock]]
}

func NewGossipSource(ctx context.Context, gossip *network.GossipManager) *GossipSource {
	g := &GossipSource{
		gossip:       gossip,
		gossipBlocks: gossip.SubscribeSignedBeaconBlocks(ctx),
		blocks:       btree.NewMap[uint64, chan *peers.PeeredObject[*cltypes.SignedBeaconBlock]](32),
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case recv := <-g.gossipBlocks:
				ch := g.grabOrCreate(ctx, recv.Data.Block.Slot)
				select {
				case ch <- recv:
				default:
				}
			}
		}
	}()
	return g
}

func (b *GossipSource) grabOrCreate(ctx context.Context, id uint64) chan *peers.PeeredObject[*cltypes.SignedBeaconBlock] {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch, ok := b.blocks.Get(id)
	if !ok {
		ch = make(chan *peers.PeeredObject[*cltypes.SignedBeaconBlock], 3)
		b.blocks.Set(id, ch)
	}
	return ch
}
func (b *GossipSource) GetRange(_ *sql.Tx, ctx context.Context, from uint64, count uint64) ([]*peers.PeeredObject[*cltypes.SignedBeaconBlock], error) {
	out := make([]*peers.PeeredObject[*cltypes.SignedBeaconBlock], 0, count)
	for i := from; i < from+count; i++ {
		ch := b.grabOrCreate(ctx, i)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case item := <-ch:
			out = append(out, item)
		}
	}
	return out, nil
}

func (b *GossipSource) PurgeRange(_ *sql.Tx, ctx context.Context, from uint64, count uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocks.AscendMut(from, func(key uint64, value chan *peers.PeeredObject[*cltypes.SignedBeaconBlock]) bool {
		if key >= from+count {
			return false
		}
		b.blocks.Delete(key)
		return true
	})
	return nil
}
