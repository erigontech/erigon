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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
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
	h5, _ := encodeSyntheticHeader(t, 5)
	newest, single, valid := processHeaderBatch([][]byte{h5})
	require.True(t, valid)
	require.True(t, single)
	require.Equal(t, uint64(5), newest.num)

	// Out-of-order multi-header batch: newest is the highest number, not the first entry.
	h6, _ := encodeSyntheticHeader(t, 6)
	h7, hash7 := encodeSyntheticHeader(t, 7)
	newest, single, valid = processHeaderBatch([][]byte{h7, h6})
	require.True(t, valid)
	require.False(t, single)
	require.Equal(t, uint64(7), newest.num)
	require.Equal(t, hash7, newest.hash)

	_, _, valid = processHeaderBatch(nil)
	require.False(t, valid, "empty batch is not usable")

	_, _, valid = processHeaderBatch([][]byte{{0xff, 0xff, 0xff}})
	require.False(t, valid, "decode error is not usable")
}

func TestWitnessCacheShouldBuild(t *testing.T) {
	cases := []struct {
		name          string
		num           uint64
		single        bool
		freshest      uint64
		alreadyCached bool
		want          bool
	}{
		{"clean single advance", 10, true, 10, false, true},
		{"already cached", 10, true, 10, true, false},
		{"multi-header catch-up batch", 10, false, 10, false, false},
		{"superseded by newer tip", 10, true, 11, false, false},
		{"reorged head, uncached hash rebuilds", 8, true, 8, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldBuild(tc.num, tc.single, tc.freshest, tc.alreadyCached))
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

func TestDecidePin(t *testing.T) {
	t.Parallel()
	parent := hashN(0x11)
	other := hashN(0x22)
	cases := []struct {
		name                string
		havePin             bool
		pinNum              uint64
		pinHash             common.Hash
		blockNum            uint64
		canonicalParentHash common.Hash
		want                pinVerdict
	}{
		{"no pin yet (cold start)", false, 0, common.Hash{}, 5, parent, pinStale},
		{"pin at B-1 with canonical hash builds", true, 4, parent, 5, parent, pinUsable},
		{"pin lags by two (tip jumped a coalesced gap)", true, 3, parent, 5, parent, pinStale},
		{"pin rolled ahead of the parent", true, 5, parent, 5, parent, pinStale},
		{"pin at B-1 but parent reorged to a new hash", true, 4, other, 5, parent, pinStale},
		{"genesis block has no parent to pin", true, 0, parent, 0, parent, pinStale},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, decidePin(tc.havePin, tc.pinNum, tc.pinHash, tc.blockNum, tc.canonicalParentHash))
		})
	}
}

// TestOpenRollingPin pins the current committed head against a real temporal DB and
// asserts the pin tags that head's number and canonical hash; close is idempotent.
func TestOpenRollingPin(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()

	pin, err := openRollingPin(ctx, m.DB)
	require.NoError(t, err)
	require.NotNil(t, pin)

	roTx, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	head, err := stages.GetStageProgress(roTx, stages.Finish)
	require.NoError(t, err)
	require.Equal(t, head, pin.num, "pin tags the committed head")
	wantHash, err := rawdb.ReadCanonicalHash(roTx, head)
	require.NoError(t, err)
	require.Equal(t, wantHash, pin.hash, "pin tags the head's canonical hash")

	pin.close()
	pin.close() // idempotent
}

// TestBuildAndCacheHeadCaptureStalePin drives the head-capture build with no held pin.
// The parent-commitment gate rejects it, so nothing is cached and the pin is
// re-established at the committed head for the next block.
func TestBuildAndCacheHeadCaptureStalePin(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))

	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false)
	api.witnessCache = newWitnessResultCache(96, 0, true, true)

	const blockNum = uint64(3)
	hash, _ := buildTestChainHeader(t, m, blockNum)

	next := api.buildAndCacheHeadCapture(ctx, nil, blockNum, hash)
	defer next.close()

	require.False(t, api.witnessCache.Contains(hash), "a stale (nil) pin must cache nothing")
	require.Equal(t, 0, api.witnessCache.Len())
	require.NotNil(t, next, "the pin is re-established at the committed head")
}

// TestBuildAndCacheHeadCaptureHappyPath commits blocks 1..B-1, pins that snapshot as
// parent(B), commits B, then builds B's witness head-capture. The pinned-parent build
// must populate the cache and be byte-identical to the durable on-demand build that reads
// the same parent commitment from history.
func TestBuildAndCacheHeadCaptureHappyPath(t *testing.T) {
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	m, testChain := rpcdaemontest.CreateTestExecModuleNoInsert(t)
	ctx := context.Background()
	require.NoError(t, m.DB.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))

	const buildNum = uint64(6)
	require.NoError(t, m.InsertChain(testChain.Slice(0, int(buildNum)-1)))

	pin, err := openRollingPin(ctx, m.DB)
	require.NoError(t, err)
	require.NotNil(t, pin)
	require.Equal(t, buildNum-1, pin.num, "pin sits one block behind the block to build")

	require.NoError(t, m.InsertChain(testChain.Slice(int(buildNum)-1, int(buildNum))))

	hash, _ := buildTestChainHeader(t, m, buildNum)

	api := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false)
	api.witnessCache = newWitnessResultCache(96, 0, true, true)

	onDemand := NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, 0, false)
	bn := rpc.BlockNumber(buildNum)
	want, err := onDemand.ExecutionWitness(ctx, rpc.BlockNumberOrHash{BlockNumber: &bn}, nil)
	require.NoError(t, err, "durable on-demand build must succeed")

	next := api.buildAndCacheHeadCapture(ctx, pin, buildNum, hash)
	defer next.close()

	cached, ok := api.witnessCache.Get(hash)
	require.True(t, ok, "head-capture build must populate the cache")

	wantBytes, err := want.MarshalFastJSON()
	require.NoError(t, err)
	gotBytes, err := cached.MarshalFastJSON()
	require.NoError(t, err)
	require.Equal(t, wantBytes, gotBytes, "head-capture witness must match the durable on-demand build")
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

	cache := newWitnessResultCache(96, 0, false, false)
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
		cached, _ = cache.Get(hash)
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
