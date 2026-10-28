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
	"testing"

	"github.com/stretchr/testify/require"

	bscchain "github.com/erigontech/erigon/bsc/chain"
	"github.com/erigontech/erigon/bsc/parlia"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

type attestedChainDB struct {
	fakeChainDB
	attestations map[common.Hash]*parlia.VoteData
}

func (db attestedChainDB) Snapshot(hash common.Hash, number uint64) (*parlia.Snapshot, error) {
	snap, err := db.fakeChainDB.Snapshot(hash, number)
	if err != nil {
		return nil, err
	}
	snap.Attestation = db.attestations[hash]
	return snap, nil
}

func newFakeDBChain(t *testing.T, length int) (attestedChainDB, []*types.Header) {
	t.Helper()
	test, root := newConnectCCBTest(t)
	headers := append([]*types.Header{root}, test.makeHeaders(root, make([]uint64, length))...)
	for _, h := range headers {
		h.Difficulty.SetUint64(2)
	}
	for i := 1; i < len(headers); i++ {
		headers[i].ParentHash = headers[i-1].Hash()
	}
	db := attestedChainDB{fakeChainDB: fakeChainDB{}, attestations: map[common.Hash]*parlia.VoteData{}}
	db.add(headers...)
	return db, headers
}

func TestInitialiseCCBRootIsHeadMinusDepth(t *testing.T) {
	t.Parallel()
	db, headers := newFakeDBChain(t, 100)
	head := headers[100]
	db.attestations[head.Hash()] = attest(headers[98], headers[99])

	ccb, err := InitialiseCCB(t.Context(), db, newFakeVerifier(), head)
	require.NoError(t, err)
	require.Equal(t, headers[100-ccbInitDepth].Hash(), ccb.Root().Hash())
	require.Equal(t, head.Hash(), ccb.Tip().Hash())
	require.Len(t, ccb.Headers(), ccbInitDepth+1)
}

func TestInitialiseCCBRootIsOlderFinalized(t *testing.T) {
	t.Parallel()
	db, headers := newFakeDBChain(t, 100)
	head := headers[100]
	db.attestations[head.Hash()] = attest(headers[10], headers[11])

	ccb, err := InitialiseCCB(t.Context(), db, newFakeVerifier(), head)
	require.NoError(t, err)
	require.Equal(t, headers[10].Hash(), ccb.Root().Hash())
	require.Equal(t, head.Hash(), ccb.Tip().Hash())
}

func TestInitialiseCCBShortChain(t *testing.T) {
	t.Parallel()
	db, headers := newFakeDBChain(t, 10)

	ccb, err := InitialiseCCB(t.Context(), db, newFakeVerifier(), headers[10])
	require.NoError(t, err)
	require.Equal(t, headers[0].Hash(), ccb.Root().Hash())
	require.Equal(t, headers[10].Hash(), ccb.Tip().Hash())
}

func TestInitialiseCCBMissingHeader(t *testing.T) {
	t.Parallel()
	db, headers := newFakeDBChain(t, 100)
	delete(db.fakeChainDB, headers[50].Hash())

	_, err := InitialiseCCB(t.Context(), db, newFakeVerifier(), headers[100])
	require.ErrorIs(t, err, parlia.ErrUnknownAncestor)
}

func TestInitialiseCCBChapel(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	db := fakeChainDB{}
	db.add(headers...)
	store, err := parlia.NewSnapshotStore(bscchain.Chapel.Config, db, 4096)
	require.NoError(t, err)
	head := headers[len(headers)-1]

	ccb, err := InitialiseCCB(t.Context(), store, NewParliaVerifier(bscchain.Chapel.Config), head)
	require.NoError(t, err)
	require.Equal(t, head.Number.Uint64()-ccbInitDepth, ccb.Root().Number.Uint64())
	require.Equal(t, head.Hash(), ccb.Tip().Hash())

	snap, err := store.Snapshot(head.Hash(), head.Number.Uint64())
	require.NoError(t, err)
	finalizedNum, finalizedHash := snap.Finalized()
	require.Equal(t, finalizedHash, ccb.Finalized().Hash())
	require.Equal(t, finalizedNum, ccb.Finalized().Number.Uint64())

	require.NoError(t, ccb.PruneToFinalized())
	require.Equal(t, finalizedHash, ccb.Root().Hash())
}
