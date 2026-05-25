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

package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// fakeUnwindDomainPutter captures DomainPut so we can assert
// Provider.Unwind reached sub-op #2 (or skipped it on a guard
// failure) without standing up a real SharedDomains.
type fakeUnwindDomainPutter struct {
	calls    int
	gotKey   []byte
	gotValue []byte
	gotTxNum uint64
}

func (f *fakeUnwindDomainPutter) DomainPut(domain kv.Domain, tx kv.TemporalTx, k, v []byte, txNum uint64, prevVal []byte) error {
	f.calls++
	f.gotKey = k
	f.gotValue = v
	f.gotTxNum = txNum
	return nil
}

// stubTx satisfies kv.TemporalTx for the purpose of being non-nil; no
// method on it is exercised — the fakeUnwindDomainPutter doesn't read
// the tx, it just captures the arg.
type stubTx struct{ kv.TemporalTx }

func TestProviderUnwind_RejectsNonAlignedChain(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	dom := &fakeUnwindDomainPutter{}
	tx := &stubTx{}

	err := p.Unwind(context.Background(), 1000, UnwindOpts{
		BlockAligned: false,
		Domains:      dom,
		Tx:           tx,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "BlockAligned=false")
	require.Equal(t, 0, dom.calls, "must short-circuit before sub-op #2 on guard failure")
}

func TestProviderUnwind_RejectsNilDomains(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	err := p.Unwind(context.Background(), 1000, UnwindOpts{
		BlockAligned: true,
		Domains:      nil,
		Tx:           &stubTx{},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "opts.Domains is nil")
}

func TestProviderUnwind_RejectsNilTx(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	err := p.Unwind(context.Background(), 1000, UnwindOpts{
		BlockAligned: true,
		Domains:      &fakeUnwindDomainPutter{},
		Tx:           nil,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "opts.Tx is nil")
}

func TestProviderUnwind_RejectsNilProvider(t *testing.T) {
	t.Parallel()
	var p *Provider
	err := p.Unwind(context.Background(), 1000, UnwindOpts{
		BlockAligned: true,
		Domains:      &fakeUnwindDomainPutter{},
		Tx:           &stubTx{},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil provider")
}

func TestProviderUnwind_AlignedHappyPath_AnchorsCommitmentAtToBlock(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	dom := &fakeUnwindDomainPutter{}
	tx := &stubTx{}
	const toBlock, txNum uint64 = 15_000, 22_500
	trie := []byte("encoded-trie-at-15000")

	require.NoError(t, p.Unwind(context.Background(), toBlock, UnwindOpts{
		BlockAligned: true,
		TxNum:        txNum,
		TrieState:    trie,
		Domains:      dom,
		Tx:           tx,
	}))

	require.Equal(t, 1, dom.calls, "sub-op #2 must run exactly once on the happy path")
	require.Equal(t, commitmentdb.KeyCommitmentState, dom.gotKey)
	require.Equal(t, txNum, dom.gotTxNum)

	// The encoded payload must round-trip with the same coordinates
	// — this is the contract every reader of KeyCommitmentState
	// depends on (DecodeTxBlockNums, LatestCommitmentState, etc.).
	gotTxNum, gotBlockNum := commitmentdb.DecodeTxBlockNums(dom.gotValue)
	require.Equal(t, txNum, gotTxNum)
	require.Equal(t, toBlock, gotBlockNum)
}
