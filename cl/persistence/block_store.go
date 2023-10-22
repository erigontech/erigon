package persistence

import (
	"context"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
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
func (b *BeaconRpcSource) PurgeRange(ctx context.Context, _ kv.RwTx, from uint64, count uint64) error {
	return nil
}

var _ BlockSource = (*GossipSource)(nil)

type GossipSource struct {
	gossip       *network.GossipManager
	gossipBlocks <-chan *peers.PeeredObject[*cltypes.SignedBeaconBlock]

	mu     sync.Mutex
	blocks *btree.Map[uint64, chan *peers.PeeredObject[*cltypes.SignedBeaconBlock]]
}

func (*GossipSource) GetBlock(ctx context.Context, tx kv.Tx, slot uint64) (*peers.PeeredObject[*cltypes.SignedBeaconBlock], error) {
	panic("unimplemented")
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
func (b *GossipSource) GetRange(ctx context.Context, _ kv.Tx, from uint64, count uint64) (*peers.PeeredObject[[]*cltypes.SignedBeaconBlock], error) {
	out := &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{}
	for i := from; i < from+count; i++ {
		ch := b.grabOrCreate(ctx, i)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case item := <-ch:
			out.Data = append(out.Data, item.Data)
			out.Peer = item.Peer
		}
	}
	return out, nil
}

func (b *GossipSource) PurgeRange(ctx context.Context, _ kv.RwTx, from uint64, count uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	initSize := count
	if initSize > 256 {
		initSize = 256
	}
	xs := make([]uint64, 0, initSize)
	b.blocks.Ascend(from, func(key uint64, value chan *peers.PeeredObject[*cltypes.SignedBeaconBlock]) bool {
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
