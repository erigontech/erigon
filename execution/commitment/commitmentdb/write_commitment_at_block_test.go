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

package commitmentdb

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// fakeDomainPutter captures DomainPut calls so we can assert the
// helper passes the right arguments and produces the right encoded
// payload, without needing a real SharedDomains harness.
type fakeDomainPutter struct {
	gotDomain kv.Domain
	gotTx     kv.TemporalTx
	gotKey    []byte
	gotValue  []byte
	gotTxNum  uint64
	gotPrev   []byte
	calls     int
	err       error
}

func (f *fakeDomainPutter) DomainPut(domain kv.Domain, tx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte) error {
	f.gotDomain = domain
	f.gotTx = tx
	f.gotKey = k
	f.gotValue = v
	f.gotTxNum = txNum
	f.gotPrev = prevVal
	f.calls++
	return f.err
}

func TestWriteCommitmentEntryAtBlock_RoutesToCommitmentDomainAtKeyCommitmentState(t *testing.T) {
	t.Parallel()
	w := &fakeDomainPutter{}
	const blockNum, txNum uint64 = 12_345, 67_890
	trie := []byte("encoded-trie-state-bytes")

	require.NoError(t, WriteCommitmentEntryAtBlock(w, nil, blockNum, txNum, trie))

	require.Equal(t, 1, w.calls)
	require.Equal(t, kv.CommitmentDomain, w.gotDomain)
	require.Equal(t, KeyCommitmentState, w.gotKey)
	require.Equal(t, txNum, w.gotTxNum)
	require.Nil(t, w.gotPrev, "prevVal must be nil — the helper is unconditional, not a CAS write")
}

func TestWriteCommitmentEntryAtBlock_EncodedValueRoundTrips(t *testing.T) {
	t.Parallel()
	w := &fakeDomainPutter{}
	const blockNum, txNum uint64 = 1_000_000, 1_500_000
	trie := []byte("some-trie-state-bytes-of-nontrivial-length")

	require.NoError(t, WriteCommitmentEntryAtBlock(w, nil, blockNum, txNum, trie))

	// The encoded value MUST round-trip through commitmentState.Decode
	// — readers (LatestCommitmentState, SeekCommitment, DecodeTxBlockNums)
	// depend on the same layout. If this assertion ever fails, every
	// reader of KeyCommitmentState is broken.
	gotTxNum, gotBlockNum := DecodeTxBlockNums(w.gotValue)
	require.Equal(t, txNum, gotTxNum)
	require.Equal(t, blockNum, gotBlockNum)

	cs := new(commitmentState)
	require.NoError(t, cs.Decode(w.gotValue))
	require.Equal(t, txNum, cs.txNum)
	require.Equal(t, blockNum, cs.blockNum)
	require.Equal(t, trie, cs.trieState)
}

func TestWriteCommitmentEntryAtBlock_EmptyTrieStateIsValid(t *testing.T) {
	t.Parallel()
	// The patricia trie can legitimately encode to an empty buffer
	// (genesis / freshly-reset state). The helper must not reject it.
	w := &fakeDomainPutter{}
	require.NoError(t, WriteCommitmentEntryAtBlock(w, nil, 0, 0, nil))

	require.Equal(t, 1, w.calls)
	gotTxNum, gotBlockNum := DecodeTxBlockNums(w.gotValue)
	require.Equal(t, uint64(0), gotTxNum)
	require.Equal(t, uint64(0), gotBlockNum)
}

func TestWriteCommitmentEntryAtBlock_PropagatesPutError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("put failed")
	w := &fakeDomainPutter{err: sentinel}
	err := WriteCommitmentEntryAtBlock(w, nil, 1, 1, []byte("x"))
	require.ErrorIs(t, err, sentinel)
}
