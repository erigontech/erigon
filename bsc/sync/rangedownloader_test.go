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
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

// makeChain returns blocks [0, n], forking from base at block len(base) with extra
// as header Extra, so a fork has different hashes from the same height on.
func makeChain(base []*types.Block, n int, extra string) []*types.Block {
	chain := slices.Clone(base)
	for i := len(chain); i <= n; i++ {
		h := &types.Header{Number: *uint256.NewInt(uint64(i)), Difficulty: *uint256.NewInt(1), Extra: []byte(extra)}
		if i > 0 {
			h.ParentHash = chain[i-1].Hash()
		}
		chain = append(chain, types.NewBlock(h, nil, nil, nil, nil, nil))
	}
	return chain
}

type fakeRangeService struct {
	mu        sync.Mutex
	peers     []*p2p.PeerId
	chains    map[p2p.PeerId][]*types.Block
	penalized []*p2p.PeerId
}

func (s *fakeRangeService) ListPeersMayHaveBlockNum(uint64) []*p2p.PeerId {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*p2p.PeerId
	for _, p := range s.peers {
		if !slices.Contains(s.penalized, p) {
			out = append(out, p)
		}
	}
	return out
}

func (s *fakeRangeService) FetchHeaders(_ context.Context, start, end uint64, peerId *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Header], error) {
	chain := s.chains[*peerId]
	end = min(end, uint64(len(chain)))
	headers := make([]*types.Header, 0, end-start)
	for _, b := range chain[start:end] {
		headers = append(headers, b.Header())
	}
	return p2p.FetcherResponse[[]*types.Header]{Data: headers}, nil
}

func (s *fakeRangeService) FetchBodies(_ context.Context, headers []*types.Header, peerId *p2p.PeerId, _ ...p2p.FetcherOption) (p2p.FetcherResponse[[]*types.Body], error) {
	chain := s.chains[*peerId]
	bodies := make([]*types.Body, len(headers))
	for i, h := range headers {
		bodies[i] = chain[h.Number.Uint64()].Body()
	}
	return p2p.FetcherResponse[[]*types.Body]{Data: bodies}, nil
}

func (s *fakeRangeService) Penalize(_ context.Context, peerId *p2p.PeerId) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.penalized = append(s.penalized, peerId)
	return nil
}

// fakeExecutor commits every inserted batch and rejects the first block that is
// not on the valid chain.
type fakeExecutor struct {
	valid []*types.Block
	head  *types.Header
}

func (e *fakeExecutor) insert(_ context.Context, blocks []*types.Block) (bool, error) {
	for _, b := range blocks {
		if b.Hash() != e.valid[b.NumberU64()].Hash() {
			return false, &badBlockError{lastValid: e.head}
		}
		e.head = b.Header()
	}
	return true, nil
}

func TestDownloadRanges_BadBlockPenalizesPeerAndRefetches(t *testing.T) {
	t.Parallel()
	good := makeChain(nil, 10, "")
	bad := makeChain(good[:5], 10, "fork")
	badPeer, goodPeer := p2p.PeerIdFromUint64(1), p2p.PeerIdFromUint64(2)
	svc := &fakeRangeService{
		peers:  []*p2p.PeerId{badPeer, goodPeer},
		chains: map[p2p.PeerId][]*types.Block{*badPeer: bad, *goodPeer: good},
	}
	exec := &fakeExecutor{valid: good, head: good[0].Header()}

	err := downloadRanges(t.Context(), log.New(), svc, 1, 10, good[0].Header(), exec.insert)

	require.NoError(t, err)
	require.Equal(t, []*p2p.PeerId{badPeer}, svc.penalized)
	require.Equal(t, good[10].Hash(), exec.head.Hash())
}

func TestDownloadRanges_UnlinkedRangePenalizesPeer(t *testing.T) {
	t.Parallel()
	good := makeChain(nil, 10, "")
	unlinked := makeChain(nil, 10, "other genesis")
	badPeer, goodPeer := p2p.PeerIdFromUint64(1), p2p.PeerIdFromUint64(2)
	svc := &fakeRangeService{
		peers:  []*p2p.PeerId{badPeer, goodPeer},
		chains: map[p2p.PeerId][]*types.Block{*badPeer: unlinked, *goodPeer: good},
	}
	exec := &fakeExecutor{valid: good, head: good[0].Header()}

	err := downloadRanges(t.Context(), log.New(), svc, 1, 10, good[0].Header(), exec.insert)

	require.NoError(t, err)
	require.Equal(t, []*p2p.PeerId{badPeer}, svc.penalized)
	require.Equal(t, good[10].Hash(), exec.head.Hash())
}

type fakeForkChoice struct {
	status  execmodule.ExecutionStatus
	headers map[common.Hash]*types.Header
	valid   common.Hash
}

func (f *fakeForkChoice) UpdateForkChoice(context.Context, common.Hash, common.Hash, common.Hash, ...uint64) (execmodule.ExecutionStatus, *string, common.Hash, error) {
	return f.status, nil, f.valid, nil
}

func (f *fakeForkChoice) GetHeaderByHash(_ context.Context, hash common.Hash) *types.Header {
	return f.headers[hash]
}

func TestCommitHead_BadBlockReportsLastValidHeader(t *testing.T) {
	t.Parallel()
	chain := makeChain(nil, 3, "")
	lastValid := chain[1].Header()
	fc := &fakeForkChoice{
		status:  execmodule.ExecutionStatusBadBlock,
		headers: map[common.Hash]*types.Header{lastValid.Hash(): lastValid},
		valid:   lastValid.Hash(),
	}

	err := commitHead(t.Context(), log.New(), fc, chain[3])

	var bad *badBlockError
	require.ErrorAs(t, err, &bad, fmt.Sprint(err))
	require.Equal(t, lastValid.Hash(), bad.lastValid.Hash())
}
