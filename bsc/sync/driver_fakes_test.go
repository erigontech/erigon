// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package bscsync

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/bsc/parlia"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

func fakeBlock(parent *types.Header, salt byte) *types.Block {
	return fakeBlockWithDifficulty(parent, salt, 2)
}

func fakeBlockWithDifficulty(parent *types.Header, salt byte, difficulty uint64) *types.Block {
	h := &types.Header{
		TxHash:      empty.RootHash,
		ReceiptHash: empty.RootHash,
		UncleHash:   empty.UncleHash,
		Difficulty:  *uint256.NewInt(difficulty),
		Extra:       []byte{salt},
	}
	if parent != nil {
		h.ParentHash = parent.Hash()
		h.Number = *uint256.NewInt(parent.Number.Uint64() + 1)
		h.Time = parent.Time + 1
	}
	return types.NewBlockWithHeader(h, nil)
}

func fakeChain(n int) []*types.Block {
	blocks := []*types.Block{fakeBlock(nil, 0)}
	for len(blocks) < n {
		blocks = append(blocks, fakeBlock(blocks[len(blocks)-1].HeaderNoCopy(), 0))
	}
	return blocks
}

type fakeP2P struct {
	*fakeTipRegistrar
	peers int

	mu        sync.Mutex
	chain     []*types.Block
	penalized []*p2p.PeerId
}

func (f *fakeP2P) Penalize(_ context.Context, peerId *p2p.PeerId) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.penalized = append(f.penalized, peerId)
	return nil
}

func (f *fakeP2P) FetchBlocksBackwards(context.Context, common.Hash, p2p.BbdHeaderReader, ...p2p.BbdOption) (p2p.BbdResultFeed, error) {
	return p2p.BbdResultFeed{}, errors.New("fake: use fakeNetwork.fetchBackwards")
}

func (f *fakeP2P) penalties() []*p2p.PeerId {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.penalized)
}

func (f *fakeP2P) blocks() []*types.Block {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.chain
}

func (f *fakeP2P) ListPeersMayHaveBlockNum(uint64) []*p2p.PeerId {
	ids := make([]*p2p.PeerId, f.peers)
	for i := range ids {
		ids[i] = p2p.PeerIdFromUint64(uint64(i + 1))
	}
	return ids
}

func (f *fakeP2P) FetchHeaders(_ context.Context, start, end uint64, _ *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error) {
	chain := f.blocks()
	var headers []*types.Header
	for n := start; n < end && n < uint64(len(chain)); n++ {
		headers = append(headers, chain[n].HeaderNoCopy())
	}
	return p2p.FetcherResponse[[]*types.Header]{Data: headers}, nil
}

func (f *fakeP2P) FetchBodies(_ context.Context, headers []*types.Header, _ *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error) {
	bodies := make([]*types.Body, len(headers))
	for i := range bodies {
		bodies[i] = &types.Body{}
	}
	return p2p.FetcherResponse[[]*types.Body]{Data: bodies}, nil
}

type fcuCall struct {
	head, safe, finalized common.Hash
}

type fakeChainRW struct {
	mu        sync.Mutex
	headers   map[common.Hash]*types.Header
	canonical map[uint64]common.Hash
	head      *types.Header
	inserted  []*types.Block
	fcus      []fcuCall
	fcuStatus func(head common.Hash) execmodule.ExecutionStatus
}

func newFakeChainRW(canonical []*types.Block) *fakeChainRW {
	f := &fakeChainRW{headers: map[common.Hash]*types.Header{}, canonical: map[uint64]common.Hash{}}
	for _, b := range canonical {
		f.headers[b.Hash()] = b.HeaderNoCopy()
		f.canonical[b.NumberU64()] = b.Hash()
		f.head = b.HeaderNoCopy()
	}
	return f
}

func (f *fakeChainRW) CurrentHeader(context.Context) *types.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.head
}

func (f *fakeChainRW) FrozenBlocks(context.Context) (uint64, bool) { return 0, false }

func (f *fakeChainRW) GetHeaderByNumber(_ context.Context, number uint64) *types.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.headers[f.canonical[number]]
}

func (f *fakeChainRW) GetHeader(_ context.Context, hash common.Hash, number uint64) *types.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	h := f.headers[hash]
	if h == nil || h.Number.Uint64() != number {
		return nil
	}
	return h
}

func (f *fakeChainRW) GetHeaderByHash(_ context.Context, hash common.Hash) *types.Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.headers[hash]
}

func (f *fakeChainRW) InsertBlocks(_ context.Context, blocks []*types.Block) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range blocks {
		if _, ok := f.headers[b.ParentHash()]; !ok {
			return fmt.Errorf("fake: unknown parent of %d", b.NumberU64())
		}
		f.headers[b.Hash()] = b.HeaderNoCopy()
		f.inserted = append(f.inserted, b)
	}
	return nil
}

func (f *fakeChainRW) UpdateForkChoice(_ context.Context, head, safe, finalized common.Hash, _ ...uint64) (execmodule.ExecutionStatus, *string, common.Hash, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fcus = append(f.fcus, fcuCall{head: head, safe: safe, finalized: finalized})
	if f.fcuStatus != nil {
		if status := f.fcuStatus(head); status != execmodule.ExecutionStatusSuccess {
			return status, nil, common.Hash{}, nil
		}
	}
	h, ok := f.headers[head]
	if !ok {
		return execmodule.ExecutionStatusMissingSegment, nil, common.Hash{}, nil
	}
	f.head = h
	for n := range f.canonical {
		if n > h.Number.Uint64() {
			delete(f.canonical, n)
		}
	}
	for cur := h; cur != nil && f.canonical[cur.Number.Uint64()] != cur.Hash(); cur = f.headers[cur.ParentHash] {
		f.canonical[cur.Number.Uint64()] = cur.Hash()
	}
	return execmodule.ExecutionStatusSuccess, nil, common.Hash{}, nil
}

func (f *fakeChainRW) headNum() uint64 {
	return f.CurrentHeader(context.Background()).Number.Uint64()
}

func (f *fakeChainRW) lastFcu() fcuCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fcus[len(f.fcus)-1]
}

func (f *fakeChainRW) numFcus() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.fcus)
}

func (f *fakeChainRW) setFcuStatus(fn func(head common.Hash) execmodule.ExecutionStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fcuStatus = fn
}

func (f *fakeChainRW) numInserted() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.inserted)
}

func (f *fakeP2P) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

type fakeParliaDB struct {
	chainRW *fakeChainRW
}

func (db fakeParliaDB) GetHeader(hash common.Hash, number uint64) *types.Header {
	return db.chainRW.GetHeader(context.Background(), hash, number)
}

func (db fakeParliaDB) Snapshot(hash common.Hash, number uint64) (*parlia.Snapshot, error) {
	if db.GetHeader(hash, number) == nil {
		return nil, parlia.ErrUnknownAncestor
	}
	return &parlia.Snapshot{Number: number, Hash: hash}, nil
}

type fakeNetwork struct {
	mu     sync.Mutex
	blocks map[common.Hash]*types.Block
}

func newFakeNetwork(blocks ...[]*types.Block) *fakeNetwork {
	n := &fakeNetwork{blocks: map[common.Hash]*types.Block{}}
	for _, chain := range blocks {
		n.add(chain...)
	}
	return n
}

func (n *fakeNetwork) add(blocks ...*types.Block) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, b := range blocks {
		n.blocks[b.Hash()] = b
	}
}

func (n *fakeNetwork) fetchBackwards(ctx context.Context, hash common.Hash, hr p2p.BbdHeaderReader, limit uint64, _ *p2p.PeerId, consume func([]*types.Block) error) error {
	n.mu.Lock()
	var chain []*types.Block
	for b := n.blocks[hash]; b != nil; b = n.blocks[b.ParentHash()] {
		chain = append(chain, b)
		if known, _ := hr.HeaderByHash(ctx, b.ParentHash()); known != nil {
			break
		}
	}
	n.mu.Unlock()
	if len(chain) == 0 || uint64(len(chain)) > limit {
		return fmt.Errorf("fake: no chain for %x", hash)
	}
	slices.Reverse(chain)
	return consume(chain)
}
