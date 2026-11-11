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

package l1_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/eez/l1/recording"
)

var (
	registry = common.HexToAddress("0x00000000000000000000000000000000000000ee")
	// rollups7 is the input of rollups(7): `cast calldata "rollups(uint64)" 7`.
	rollups7     = hexutil.MustDecode("0xef678d270000000000000000000000000000000000000000000000000000000000000007")
	rollupOf7    = common.HexToAddress("0x00000000000000000000000000000000000000c7")
	registryCode = []byte{0x60, 0x00, 0x35, 0x00}
	rootAtF      = common.HexToHash("0xf1")
	rootAtHead   = common.HexToHash("0xf2")
)

// record is the rollups return of a rollup record, with a balance of 5 wei.
func record(contract common.Address, root common.Hash) []byte {
	out := make([]byte, 96)
	copy(out[12:32], contract[:])
	copy(out[32:64], root[:])
	out[95] = 5
	return out
}

// finality returns a chain of three blocks whose block 1, F, is finalized
// when finalized is set. At F the registry has code and answers rollups(7)
// with answer, each unless nil; at the head it holds rootAtHead.
func finality(t *testing.T, finalized bool, code, answer []byte) *recording.MemoryChain {
	t.Helper()
	block := func(n uint64, code, answer []byte) *recording.Block {
		b := &recording.Block{Header: l1.Header{Number: n, Hash: common.Hash{0xb0, byte(n)}, ParentHash: common.Hash{0xb0, byte(n - 1)}, Time: 12 * n}}
		b.Code = map[common.Address][]byte{}
		if code != nil {
			b.Code[registry] = code
		}
		if answer != nil {
			b.Calls = []recording.CallResult{{Call: l1.Call{To: registry, Data: rollups7}, Return: answer}}
		}
		return b
	}
	c, err := recording.NewMemoryChain(1, block(0, nil, nil))
	require.NoError(t, err)
	require.NoError(t, c.Append(block(1, code, answer)))
	require.NoError(t, c.Append(block(2, registryCode, record(rollupOf7, rootAtHead))))
	if finalized {
		require.NoError(t, c.Finalize(1))
	}
	return c
}

// TestFinalizedCommitment is T-SYNC-1's anchor decision on what L1 holds at
// its finalized block F.
func TestFinalizedCommitment(t *testing.T) {
	ctx := context.Background()
	f := l1.Header{Number: 1, Hash: common.Hash{0xb0, 1}, ParentHash: common.Hash{0xb0, 0}, Time: 12}
	for _, tc := range []struct {
		name         string
		finalized    bool
		code, answer []byte
		want         l1.Commitment
		err          error
	}{
		{
			name: "no finalized block", code: registryCode, answer: record(rollupOf7, rootAtF),
			want: l1.Commitment{Outcome: l1.CommitmentWait},
		},
		{
			name: "no registry code", finalized: true, code: []byte{},
			want: l1.Commitment{Outcome: l1.CommitmentSeedGenesis, Finalized: f},
		},
		{
			name: "unregistered rollup", finalized: true, code: registryCode, answer: record(common.Address{}, common.Hash{}),
			want: l1.Commitment{Outcome: l1.CommitmentSeedGenesis, Finalized: f},
		},
		{
			name: "unregistered rollup with a root", finalized: true, code: registryCode, answer: record(common.Address{}, rootAtF),
			want: l1.Commitment{Outcome: l1.CommitmentSeedGenesis, Finalized: f},
		},
		{name: "zero root", finalized: true, code: registryCode, answer: record(rollupOf7, common.Hash{}), err: l1.ErrZeroRoot},
		{name: "empty answer", finalized: true, code: registryCode, answer: []byte{}, err: l1.ErrNotRollups},
		{name: "short answer", finalized: true, code: registryCode, answer: record(rollupOf7, rootAtF)[:95], err: l1.ErrNotRollups},
		{
			name: "anchor", finalized: true, code: registryCode, answer: record(rollupOf7, rootAtF),
			want: l1.Commitment{Outcome: l1.CommitmentAnchor, Finalized: f, Root: rootAtF},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := l1.FinalizedCommitment(ctx, finality(t, tc.finalized, tc.code, tc.answer), registry, 7)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			if tc.want.Outcome == l1.CommitmentWait {
				require.ErrorIs(t, got.Cause, l1.ErrNotFound)
				got.Cause = nil
			}
			require.Equal(t, tc.want, got)
		})
	}
}

// failing fails the reads whose error is set.
type failing struct {
	*recording.MemoryChain
	finalized, code, call error
}

func (r failing) Finalized(ctx context.Context) (*l1.Header, error) {
	if r.finalized != nil {
		return nil, r.finalized
	}
	return r.MemoryChain.Finalized(ctx)
}

func (r failing) CodeAt(ctx context.Context, addr common.Address, blockHash common.Hash) ([]byte, error) {
	if r.code != nil {
		return nil, r.code
	}
	return r.MemoryChain.CodeAt(ctx, addr, blockHash)
}

func (r failing) CallAt(ctx context.Context, call l1.Call, blockHash common.Hash) ([]byte, error) {
	if r.call != nil {
		return nil, r.call
	}
	return r.MemoryChain.CallAt(ctx, call, blockHash)
}

// TestFinalizedCommitmentWaits: a read a retry can mend waits, whichever
// read fails; once the context ends, its error is returned instead.
func TestFinalizedCommitmentWaits(t *testing.T) {
	refused := fmt.Errorf("eth_call: %w", &net.OpError{Op: "dial", Net: "tcp", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}})
	reorged := fmt.Errorf("%w: %w: block 1", l1.ErrNotFound, l1.ErrNotCanonical)
	chain := finality(t, true, registryCode, record(rollupOf7, rootAtF))
	for name, r := range map[string]failing{
		"finalized block":         {MemoryChain: chain, finalized: refused},
		"code":                    {MemoryChain: chain, code: refused},
		"call":                    {MemoryChain: chain, call: refused},
		"code at a reorged block": {MemoryChain: chain, code: reorged},
		"call at a reorged block": {MemoryChain: chain, call: reorged},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := l1.FinalizedCommitment(context.Background(), r, registry, 7)
			require.NoError(t, err)
			require.Equal(t, l1.CommitmentWait, got.Outcome)
			require.Error(t, got.Cause)
			require.True(t, errors.Is(got.Cause, syscall.ECONNREFUSED) || errors.Is(got.Cause, l1.ErrNotCanonical), got.Cause)
		})
	}

	t.Run("unrecorded answer", func(t *testing.T) {
		got, err := l1.FinalizedCommitment(context.Background(), finality(t, true, registryCode, nil), registry, 7)
		require.NoError(t, err)
		require.Equal(t, l1.CommitmentWait, got.Outcome)
		require.ErrorIs(t, got.Cause, l1.ErrNotFound)
	})

	t.Run("context ended", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := l1.FinalizedCommitment(ctx, failing{MemoryChain: chain, call: ctx.Err()}, registry, 7)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// TestFinalizedCommitmentNeedsStateReads: a source without state reads
// cannot name the root, however long the node waits.
func TestFinalizedCommitmentNeedsStateReads(t *testing.T) {
	chain := finality(t, true, registryCode, record(rollupOf7, rootAtF))
	_, err := l1.FinalizedCommitment(context.Background(), struct{ l1.ChainReader }{chain}, registry, 7)
	require.ErrorIs(t, err, l1.ErrNoStateReads)
}

// TestFinalizedCommitmentErigonL1 reads the root of an Erigon L1's registry
// at its finalized block, below a head where the registry holds another.
func TestFinalizedCommitmentErigonL1(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	ctx := t.Context()
	f, err := model.Finalized(ctx)
	require.NoError(t, err)
	canonical := model.Canonical()
	root := common.BytesToHash(canonical[f.Number].Calls[0].Return[32:64])
	head := canonical[len(canonical)-1]
	require.NotEqual(t, root, common.BytesToHash(head.Calls[0].Return[32:64]))

	got, err := l1.FinalizedCommitment(ctx, l1.NewNative(m.DB, m.BlockReader, m.ReceiptsReader, m.Engine, m.ChainConfig), l1test.Registry, 1)
	require.NoError(t, err)
	require.Equal(t, l1.Commitment{Outcome: l1.CommitmentAnchor, Finalized: *f, Root: root}, got)
}
