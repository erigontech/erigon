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
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	bscchain "github.com/erigontech/erigon/bsc/chain"
	"github.com/erigontech/erigon/bsc/parlia"
	"github.com/erigontech/erigon/bsc/parlia/seal"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

const chapelFixtureRoot = 132_035_200

func loadChapelHeaders(t *testing.T) []*types.Header {
	t.Helper()
	f, err := os.Open("../parlia/testdata/chapel_headers.rlp.gz")
	require.NoError(t, err)
	defer f.Close()
	zr, err := gzip.NewReader(f)
	require.NoError(t, err)
	s := rlp.NewStream(zr, 0)
	var headers []*types.Header
	for {
		h := new(types.Header)
		if err := s.Decode(h); errors.Is(err, io.EOF) {
			break
		} else {
			require.NoError(t, err)
		}
		headers = append(headers, h)
	}
	return headers
}

func fixtureIndex(headers []*types.Header, number uint64) int {
	return int(number - headers[0].Number.Uint64())
}

func (db fakeChainDB) add(headers ...*types.Header) {
	for _, h := range headers {
		db[h.Hash()] = h
	}
}

func newChapelCCB(t *testing.T) (*CanonicalChainBuilder, []*types.Header, fakeChainDB) {
	t.Helper()
	headers := loadChapelHeaders(t)
	rootIdx := fixtureIndex(headers, chapelFixtureRoot)
	db := fakeChainDB{}
	db.add(headers[:rootIdx+1]...)
	store, err := parlia.NewSnapshotStore(bscchain.Chapel.Config, db, 4096)
	require.NoError(t, err)
	ccb, err := NewCanonicalChainBuilder(store, NewParliaVerifier(bscchain.Chapel.Config), headers[rootIdx])
	require.NoError(t, err)
	return ccb, headers[rootIdx+1:], db
}

func forgeHeader(t *testing.T, h *types.Header) *types.Header {
	t.Helper()
	forged := types.CopyHeader(h)
	forged.Extra = bytes.Clone(h.Extra)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sealHash, err := seal.Hash(forged, bscchain.Chapel.Config.ChainID)
	require.NoError(t, err)
	sig, err := crypto.Sign(sealHash[:], key)
	require.NoError(t, err)
	copy(forged.Extra[len(forged.Extra)-types.ExtraSealLength:], sig)
	return forged
}

func TestCCBChapelConnect(t *testing.T) {
	t.Parallel()
	ccb, tail, _ := newChapelCCB(t)

	for start := 0; start < len(tail); start += 100 {
		batch := tail[start:min(start+100, len(tail))]
		connected, err := ccb.Connect(t.Context(), batch)
		require.NoError(t, err)
		require.Equal(t, batch, connected)
	}
	require.Equal(t, tail[len(tail)-1].Hash(), ccb.Tip().Hash())
	require.Len(t, ccb.Headers(), len(tail)+1)
}

func TestCCBChapelForkFailsVerification(t *testing.T) {
	t.Parallel()
	ccb, tail, _ := newChapelCCB(t)
	_, err := ccb.Connect(t.Context(), tail[:50])
	require.NoError(t, err)
	tip := ccb.Tip()

	forged := forgeHeader(t, tail[40])
	child := types.CopyHeader(tail[41])
	child.ParentHash = forged.Hash()
	child = forgeHeader(t, child)

	connected, err := ccb.Connect(t.Context(), []*types.Header{forged, child})
	require.ErrorIs(t, err, parlia.ErrCoinbaseMismatch)
	require.Empty(t, connected)
	require.False(t, ccb.ContainsHash(forged.Hash()))
	require.Equal(t, tip.Hash(), ccb.Tip().Hash())
}

func TestCCBChapelPruneToFinalized(t *testing.T) {
	t.Parallel()
	ccb, tail, db := newChapelCCB(t)

	for start := 0; start < len(tail); start += 50 {
		batch := tail[start:min(start+50, len(tail))]
		_, err := ccb.Connect(t.Context(), batch)
		require.NoError(t, err)
		db.add(batch...)
		require.NoError(t, ccb.PruneToFinalized())

		tip := ccb.Tip()
		justified, finalized := ccb.Justified(), ccb.Finalized()
		require.NotNil(t, justified)
		require.NotNil(t, finalized)
		require.Equal(t, finalized.Hash(), ccb.Root().Hash())
		require.Less(t, finalized.Number.Uint64(), justified.Number.Uint64())
		require.Less(t, justified.Number.Uint64(), tip.Number.Uint64())
		require.LessOrEqual(t, tip.Number.Uint64()-finalized.Number.Uint64(), uint64(10))
	}
	require.Equal(t, tail[len(tail)-1].Hash(), ccb.Tip().Hash())
}
