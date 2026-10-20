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

	bscchain "github.com/erigontech/erigon/bsc/chain"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

type fixtureReader map[common.Hash]*types.Header

func newFixtureReader(headers []*types.Header) fixtureReader {
	r := make(fixtureReader, len(headers))
	for _, h := range headers {
		r[h.Hash()] = h
	}
	return r
}

func (r fixtureReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	h, ok := r[hash]
	if !ok || h.Number.Uint64() != number {
		return nil
	}
	return h
}

func chapelConfig(t *testing.T) *chain.Config {
	t.Helper()
	return bscchain.Chapel.Config
}

func firstCheckpoint(headers []*types.Header) uint64 {
	first := headers[0].Number.Uint64()
	cp := first - first%epochLength + checkpointOffset
	if cp-checkpointOffset < first {
		cp += epochLength
	}
	return cp
}

func bootstrapFixture(t *testing.T, headers []*types.Header) *Snapshot {
	t.Helper()
	cp := firstCheckpoint(headers)
	snap, err := NewCheckpointSnapshot(chapelConfig(t), headerAt(t, headers, cp), headerAt(t, headers, cp-checkpointOffset))
	require.NoError(t, err)
	return snap
}

func TestNewCheckpointSnapshot(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	cp := firstCheckpoint(headers)
	snap := bootstrapFixture(t, headers)

	require.Equal(t, cp, snap.Number)
	require.Equal(t, headerAt(t, headers, cp).Hash(), snap.Hash)
	validators, _, err := ParseValidators(headerAt(t, headers, cp-checkpointOffset), epochLength)
	require.NoError(t, err)
	require.Len(t, snap.Validators, len(validators))
	require.Positive(t, snap.TurnLength)
	require.Empty(t, snap.Recents)
	require.Nil(t, snap.Attestation)
}

func TestNewCheckpointSnapshotRejectsNonCheckpoint(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	cp := firstCheckpoint(headers)

	_, err := NewCheckpointSnapshot(chapelConfig(t), headerAt(t, headers, cp+1), headerAt(t, headers, cp-checkpointOffset))
	require.ErrorIs(t, err, ErrNotCheckpoint)
	_, err = NewCheckpointSnapshot(chapelConfig(t), headerAt(t, headers, cp), headerAt(t, headers, cp-checkpointOffset+1))
	require.ErrorIs(t, err, ErrNotCheckpoint)
}

func TestSnapshotApplyFixture(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	cp := firstCheckpoint(headers)
	snap := bootstrapFixture(t, headers)
	cfg := chapelConfig(t)

	rest := headers[cp-headers[0].Number.Uint64()+1:]
	applied, err := snap.Apply(cfg, rest, newFixtureReader(headers))
	require.NoError(t, err)

	head := headers[len(headers)-1]
	require.Equal(t, head.Number.Uint64(), applied.Number)
	require.Equal(t, head.Hash(), applied.Hash)
	require.Equal(t, cp, snap.Number, "Apply must not modify the receiver")

	justified, justifiedHash := applied.Justified()
	finalized, finalizedHash := applied.Finalized()
	require.Less(t, finalized, justified)
	require.Less(t, justified, applied.Number)
	require.Less(t, applied.Number-finalized, uint64(10))
	require.Equal(t, headerAt(t, headers, justified).Hash(), justifiedHash)
	require.Equal(t, headerAt(t, headers, finalized).Hash(), finalizedHash)
}

func TestSnapshotApplySwitchesValidatorSet(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	cp := firstCheckpoint(headers)
	snap := bootstrapFixture(t, headers)
	cfg := chapelConfig(t)
	reader := newFixtureReader(headers)

	nextEpoch := cp - checkpointOffset + epochLength
	switchAt := nextEpoch + snap.minerHistoryCheckLen()
	first := headers[0].Number.Uint64()
	require.Less(t, switchAt-first, uint64(len(headers)), "fixture must include a validator-set switch")

	before, err := snap.Apply(cfg, headers[cp-first+1:switchAt-first], reader)
	require.NoError(t, err)
	after, err := before.Apply(cfg, headers[switchAt-first:switchAt-first+1], reader)
	require.NoError(t, err)

	want, _, err := ParseValidators(headerAt(t, headers, nextEpoch), epochLength)
	require.NoError(t, err)
	require.Len(t, after.Validators, len(want))
	for _, v := range want {
		require.Contains(t, after.Validators, v)
	}
	require.Len(t, after.Recents, 1, "BEP-404 clears the recents on a switch")
}

func TestSnapshotApplyRejectsGap(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	cp := firstCheckpoint(headers)
	snap := bootstrapFixture(t, headers)
	first := headers[0].Number.Uint64()

	_, err := snap.Apply(chapelConfig(t), headers[cp-first+2:cp-first+5], newFixtureReader(headers))
	require.ErrorIs(t, err, ErrOutOfRangeChain)
}
