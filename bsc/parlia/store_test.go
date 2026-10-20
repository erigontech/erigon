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

package parlia

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

type countingReader struct {
	fixtureReader
	calls int
}

func (r *countingReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	r.calls++
	return r.fixtureReader.GetHeader(hash, number)
}

func newTestStore(t *testing.T) (*SnapshotStore, *countingReader, []*types.Header) {
	t.Helper()
	headers := loadChapelHeaders(t)
	reader := &countingReader{fixtureReader: newFixtureReader(headers)}
	store, err := NewSnapshotStore(chapelConfig(t), reader, 2048)
	require.NoError(t, err)
	return store, reader, headers
}

func TestSnapshotStoreBootstrapsFromCheckpoint(t *testing.T) {
	t.Parallel()
	store, _, headers := newTestStore(t)
	head := headers[len(headers)-1]

	snap, err := store.Snapshot(head.Hash(), head.Number.Uint64())
	require.NoError(t, err)
	require.Equal(t, head.Hash(), snap.Hash)

	c, _ := newFixtureChain(t)
	want := c.snapshots[head.Hash()]
	require.Equal(t, want.Validators, snap.Validators)
	require.Equal(t, want.TurnLength, snap.TurnLength)
	require.Equal(t, want.Attestation, snap.Attestation)
}

func TestSnapshotStoreCachesAncestors(t *testing.T) {
	t.Parallel()
	store, reader, headers := newTestStore(t)
	head := headers[len(headers)-1]
	_, err := store.Snapshot(head.Hash(), head.Number.Uint64())
	require.NoError(t, err)

	reader.calls = 0
	parent := headerAt(t, headers, head.Number.Uint64()-3)
	snap, err := store.Snapshot(parent.Hash(), parent.Number.Uint64())
	require.NoError(t, err)
	require.Equal(t, parent.Hash(), snap.Hash)
	require.Zero(t, reader.calls)
}

func TestSnapshotStoreUnknownAncestor(t *testing.T) {
	t.Parallel()
	store, _, headers := newTestStore(t)
	h := headerAt(t, headers, firstCheckpoint(headers)+epochLength-1)

	_, err := store.Snapshot(h.Hash(), h.Number.Uint64())
	require.ErrorIs(t, err, ErrUnknownAncestor)
}

func TestSnapshotStoreVerifiesHeaders(t *testing.T) {
	t.Parallel()
	store, _, headers := newTestStore(t)
	v := NewVerifier(chapelConfig(t))
	from := firstCheckpoint(headers) + epochLength + 2

	for _, h := range headers[from-headers[0].Number.Uint64():] {
		require.NoError(t, v.VerifyHeader(store, h), "block %d", h.Number.Uint64())
	}
}

func TestSnapshotStoreJustifiedAndFinalized(t *testing.T) {
	t.Parallel()
	store, _, headers := newTestStore(t)
	head := headers[len(headers)-1]

	justified, err := store.Justified(head.Hash(), head.Number.Uint64())
	require.NoError(t, err)
	finalized, err := store.Finalized(head.Hash(), head.Number.Uint64())
	require.NoError(t, err)
	require.Less(t, finalized.Number.Uint64(), justified.Number.Uint64())
	require.Less(t, justified.Number.Uint64(), head.Number.Uint64())
	require.Equal(t, headerAt(t, headers, justified.Number.Uint64()).Hash(), justified.Hash())
	require.Equal(t, headerAt(t, headers, finalized.Number.Uint64()).Hash(), finalized.Hash())
}
