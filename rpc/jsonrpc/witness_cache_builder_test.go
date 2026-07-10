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

package jsonrpc

import (
	"context"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// encodeSyntheticHeader RLP-encodes a header carrying only a block number, as the
// header subscription would deliver it, and returns its bytes and canonical hash.
func encodeSyntheticHeader(t *testing.T, num uint64) ([]byte, common.Hash) {
	t.Helper()
	h := &types.Header{Number: *uint256.NewInt(num)}
	b, err := rlp.EncodeToBytes(h)
	require.NoError(t, err)
	return b, h.Hash()
}

func TestDecodeHeaderRefs(t *testing.T) {
	h10, hash10 := encodeSyntheticHeader(t, 10)

	refs, err := decodeHeaderRefs([][]byte{h10})
	require.NoError(t, err)
	require.Len(t, refs, 1)
	require.Equal(t, uint64(10), refs[0].num)
	require.Equal(t, hash10, refs[0].hash)

	refs, err = decodeHeaderRefs([][]byte{{}, h10, {}})
	require.NoError(t, err)
	require.Len(t, refs, 1, "empty payloads are skipped")

	_, err = decodeHeaderRefs([][]byte{{0xff, 0xff, 0xff}})
	require.Error(t, err, "garbage RLP is a decode error")
}

func TestProcessHeaderBatch(t *testing.T) {
	cache := newWitnessCache(96, 1024)

	h5, _ := encodeSyntheticHeader(t, 5)
	newest, single, valid := processHeaderBatch(cache, [][]byte{h5})
	require.True(t, valid)
	require.True(t, single)
	require.Equal(t, uint64(5), newest.num)

	// Out-of-order multi-header batch: newest is the highest number, not the first entry.
	h6, _ := encodeSyntheticHeader(t, 6)
	h7, hash7 := encodeSyntheticHeader(t, 7)
	newest, single, valid = processHeaderBatch(cache, [][]byte{h7, h6})
	require.True(t, valid)
	require.False(t, single)
	require.Equal(t, uint64(7), newest.num)
	require.Equal(t, hash7, newest.hash)

	_, _, valid = processHeaderBatch(cache, nil)
	require.False(t, valid, "empty batch is not usable")

	_, _, valid = processHeaderBatch(cache, [][]byte{{0xff, 0xff, 0xff}})
	require.False(t, valid, "decode error is not usable")
}

func TestWitnessCacheShouldBuild(t *testing.T) {
	cases := []struct {
		name     string
		num      uint64
		single   bool
		lastSeen uint64
		frozen   uint64
		want     bool
	}{
		{"clean single advance", 10, true, 10, 9, true},
		{"already built", 10, true, 10, 10, false},
		{"multi-header reorg batch", 10, false, 10, 9, false},
		{"superseded by newer tip", 10, true, 11, 9, false},
		{"stale reorg below frozen", 8, true, 8, 10, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldBuild(tc.num, tc.single, tc.lastSeen, tc.frozen))
		})
	}
}

func TestWitnessCacheDecideCommittedHead(t *testing.T) {
	want := hashN(0xaa)
	other := hashN(0xbb)

	require.Equal(t, headWait, decideCommittedHead(4, 5, common.Hash{}, want),
		"committed head below num must wait")
	require.Equal(t, headBuild, decideCommittedHead(5, 5, want, want),
		"head reached num with matching hash must build")
	require.Equal(t, headBuild, decideCommittedHead(7, 5, want, want),
		"head past num with matching hash must build")
	require.Equal(t, headReorged, decideCommittedHead(5, 5, other, want),
		"head reached num with a different hash means reorged away")
}

// buildTestChainHeader returns the canonical hash of blockNum and its RLP-encoded
// header, as the header subscription would deliver it.
func buildTestChainHeader(t *testing.T, m *execmoduletester.ExecModuleTester, blockNum uint64) (common.Hash, []byte) {
	t.Helper()
	var (
		hash      common.Hash
		headerRLP []byte
	)
	err := m.DB.View(m.Ctx, func(tx kv.Tx) error {
		h, _, err := m.BlockReader.CanonicalHash(m.Ctx, tx, blockNum)
		if err != nil {
			return err
		}
		hash = h
		header, err := m.BlockReader.Header(m.Ctx, tx, hash, blockNum)
		if err != nil {
			return err
		}
		headerRLP, err = rlp.EncodeToBytes(header)
		return err
	})
	require.NoError(t, err)
	return hash, headerRLP
}

// TestWitnessCacheBuilderParity drives the full builder path against the test exec
// module and asserts the cached witness bytes are identical to the on-demand build.
func TestWitnessCacheBuilderParity(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := m.DB.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	})
	require.NoError(t, err)

	onDemand := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false)

	cache := newWitnessCache(96, 1024)
	builder := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false)
	builder.witnessCache = cache

	headerCh := make(chan [][]byte, 8)
	builderDone := make(chan struct{})
	go func() { defer close(builderDone); RunWitnessCacheBuilder(ctx, builder, headerCh) }()
	// Join the builder before the test module's DB is torn down so no in-flight build
	// touches a closed DB under -race.
	defer func() { cancel(); <-builderDone }()

	const blockNum = uint64(3)
	hash, headerRLP := buildTestChainHeader(t, m, blockNum)

	headerCh <- [][]byte{headerRLP}

	var cached *ExecutionWitnessResult
	require.Eventually(t, func() bool {
		cached, _ = cache.get(blockNum, hash)
		return cached != nil
	}, 30*time.Second, 20*time.Millisecond, "builder must populate the cache")

	bn := rpc.BlockNumber(blockNum)
	want, err := onDemand.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &bn}, nil)
	require.NoError(t, err)

	// Compare the served form (rpc.fastJSONResult path): the cache stores a shell
	// carrying only pre-marshaled bytes, so MarshalFastJSON is what a hit serves.
	wantBytes, err := want.MarshalFastJSON()
	require.NoError(t, err)
	gotBytes, err := cached.MarshalFastJSON()
	require.NoError(t, err)
	require.Equal(t, wantBytes, gotBytes, "builder-path witness must be byte-identical to on-demand")
}

// TestWitnessCacheBuilderReorgEviction seeds a stale entry under a wrong hash, feeds the
// real canonical header, and asserts the builder evicts the stale entry (a request for
// the old hash now misses and falls through) while caching the real (num, hash).
func TestWitnessCacheBuilderReorgEviction(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := m.DB.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	})
	require.NoError(t, err)

	cache := newWitnessCache(96, 1024)
	builder := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false)
	builder.witnessCache = cache

	const blockNum = uint64(4)
	staleHash := hashN(0xba)
	cache.put(blockNum, staleHash, &ExecutionWitnessResult{State: []hexutil.Bytes{{0x01}}})

	headerCh := make(chan [][]byte, 8)
	builderDone := make(chan struct{})
	go func() { defer close(builderDone); RunWitnessCacheBuilder(ctx, builder, headerCh) }()
	// Join the builder before the test module's DB is torn down so no in-flight build
	// touches a closed DB under -race.
	defer func() { cancel(); <-builderDone }()

	hash, headerRLP := buildTestChainHeader(t, m, blockNum)
	require.NotEqual(t, staleHash, hash, "seed hash must differ from the canonical hash")

	headerCh <- [][]byte{headerRLP}

	require.Eventually(t, func() bool {
		r, ok := cache.get(blockNum, hash)
		return ok && r != nil
	}, 30*time.Second, 20*time.Millisecond, "builder must cache the real (num, hash)")

	_, ok := cache.get(blockNum, staleHash)
	require.False(t, ok, "stale entry under the old hash must be evicted so the request falls through")
}
