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
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/bsc/parlia/seal"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

type fixtureChain struct {
	fixtureReader
	snapshots map[common.Hash]*Snapshot
}

func (c *fixtureChain) Snapshot(hash common.Hash, number uint64) (*Snapshot, error) {
	snap, ok := c.snapshots[hash]
	if !ok || snap.Number != number {
		return nil, ErrUnknownAncestor
	}
	return snap, nil
}

func newFixtureChain(t *testing.T) (*fixtureChain, []*types.Header) {
	t.Helper()
	headers := loadChapelHeaders(t)
	reader := newFixtureReader(headers)
	snap := bootstrapFixture(t, headers)
	c := &fixtureChain{fixtureReader: reader, snapshots: map[common.Hash]*Snapshot{snap.Hash: snap}}
	for _, h := range headers[snap.Number-headers[0].Number.Uint64()+1:] {
		next, err := snap.Apply(chapelConfig(t), []*types.Header{h}, reader)
		require.NoError(t, err)
		c.snapshots[next.Hash] = next
		snap = next
	}
	return c, headers
}

func verifiableHeaders(t *testing.T, headers []*types.Header) []*types.Header {
	t.Helper()
	return headers[firstCheckpoint(headers)-headers[0].Number.Uint64()+2:]
}

func TestVerifyHeaderFixture(t *testing.T) {
	t.Parallel()
	c, headers := newFixtureChain(t)
	v := NewVerifier(chapelConfig(t))

	var outOfTurn int
	for _, h := range verifiableHeaders(t, headers) {
		require.NoError(t, v.VerifyHeader(c, h), "block %d", h.Number.Uint64())
		if h.Difficulty.Uint64() == 1 {
			outOfTurn++
		}
	}
	require.Positive(t, outOfTurn)
}

func inTurnHeader(t *testing.T, headers []*types.Header) *types.Header {
	t.Helper()
	for _, h := range verifiableHeaders(t, headers)[100:] {
		if h.Difficulty.Uint64() == 2 && h.Number.Uint64()%epochLength != 0 {
			return h
		}
	}
	t.Fatal("no in-turn header")
	return nil
}

func copyHeader(h *types.Header) *types.Header {
	cpy := types.CopyHeader(h)
	cpy.Extra = bytes.Clone(h.Extra)
	return cpy
}

func resign(t *testing.T, h *types.Header) *types.Header {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sealHash, err := seal.Hash(h, chapelConfig(t).ChainID)
	require.NoError(t, err)
	sig, err := crypto.Sign(sealHash[:], key)
	require.NoError(t, err)
	copy(h.Extra[len(h.Extra)-extraSeal:], sig)
	return h
}

func withAttestation(t *testing.T, h *types.Header, change func(*VoteAttestation)) *types.Header {
	t.Helper()
	att, err := ParseVoteAttestation(h, epochLength)
	require.NoError(t, err)
	require.NotNil(t, att)
	change(att)
	enc, err := rlp.EncodeToBytes(att)
	require.NoError(t, err)
	cpy := copyHeader(h)
	cpy.Extra = append(append(append([]byte{}, h.Extra[:extraVanity]...), enc...), h.Extra[len(h.Extra)-extraSeal:]...)
	return cpy
}

func TestVerifyHeaderWrongSigner(t *testing.T) {
	t.Parallel()
	c, headers := newFixtureChain(t)
	h := resign(t, copyHeader(inTurnHeader(t, headers)))

	require.ErrorIs(t, NewVerifier(chapelConfig(t)).VerifyHeader(c, h), ErrCoinbaseMismatch)
}

func TestVerifyHeaderWrongDifficulty(t *testing.T) {
	t.Parallel()
	c, headers := newFixtureChain(t)
	h := copyHeader(inTurnHeader(t, headers))
	h.Difficulty.SetUint64(1)

	require.ErrorIs(t, NewVerifier(chapelConfig(t)).VerifyHeader(c, h), ErrWrongDifficulty)
}

func TestVerifyHeaderRecentlySigned(t *testing.T) {
	t.Parallel()
	c, headers := newFixtureChain(t)
	h := inTurnHeader(t, headers)
	parent := c.snapshots[h.ParentHash]
	recent := parent.copy()
	for i := range uint64(recent.TurnLength) {
		recent.Recents[parent.Number-i] = h.Coinbase
	}
	c.snapshots[h.ParentHash] = recent

	require.ErrorIs(t, NewVerifier(chapelConfig(t)).VerifyHeader(c, h), ErrRecentlySigned)
}

func TestVerifyHeaderCorruptAggregateSignature(t *testing.T) {
	t.Parallel()
	c, headers := newFixtureChain(t)
	h := inTurnHeader(t, headers)
	other, err := ParseVoteAttestation(headerAt(t, headers, h.Number.Uint64()+1), epochLength)
	require.NoError(t, err)
	h = withAttestation(t, h, func(att *VoteAttestation) { att.AggSignature = other.AggSignature })

	err = NewVerifier(chapelConfig(t)).VerifyHeader(c, h)
	require.ErrorIs(t, err, ErrInvalidAttestation)
	require.ErrorContains(t, err, "signature")
}

func TestVerifyHeaderAttestationSourceMismatch(t *testing.T) {
	t.Parallel()
	c, headers := newFixtureChain(t)
	h := withAttestation(t, inTurnHeader(t, headers), func(att *VoteAttestation) {
		att.Data.SourceNumber--
		att.Data.SourceHash = headerAt(t, headers, att.Data.SourceNumber).Hash()
	})

	err := NewVerifier(chapelConfig(t)).VerifyHeader(c, h)
	require.ErrorIs(t, err, ErrInvalidAttestation)
	require.ErrorContains(t, err, "source")
}

func TestVerifyHeaderTamperedExtra(t *testing.T) {
	t.Parallel()
	c, headers := newFixtureChain(t)
	h := copyHeader(inTurnHeader(t, headers))
	h.Extra[0] ^= 0xff

	require.ErrorIs(t, NewVerifier(chapelConfig(t)).VerifyHeader(c, h), ErrCoinbaseMismatch)
}

func TestVerifyHeaderFutureTime(t *testing.T) {
	t.Parallel()
	c, headers := newFixtureChain(t)
	h := inTurnHeader(t, headers)
	v := NewVerifier(chapelConfig(t))
	v.now = func() time.Time { return time.Unix(int64(h.Time)-1, 0) }

	require.ErrorIs(t, v.VerifyHeader(c, h), ErrFutureBlock)
}

func TestVerifyHeaderOutOfTurnTooEarly(t *testing.T) {
	t.Parallel()
	c, headers := newFixtureChain(t)
	var h *types.Header
	for _, candidate := range verifiableHeaders(t, headers) {
		if candidate.Difficulty.Uint64() == diffNoTurn {
			h = copyHeader(candidate)
			break
		}
	}
	require.NotNil(t, h)
	ms := MilliTimestamp(h) - 1
	h.Time = ms / 1000
	h.MixDigest = common.Hash{}
	h.MixDigest[30], h.MixDigest[31] = byte(ms%1000>>8), byte(ms%1000)

	require.ErrorIs(t, NewVerifier(chapelConfig(t)).VerifyHeader(c, h), ErrBlockTooEarly)
}
