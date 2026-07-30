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

package commitment

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// pbinStrictWriteContext mirrors the domain's write contract: SharedDomains
// refuses a nil value outright, so a PutBranch handing one over fails here the
// way it would over a real datadir. Accepted writes are recorded in order with
// the prevData the engine claimed.
type pbinStrictWriteContext struct {
	*MockState
	puts []pbinRecordedPut
}

type pbinRecordedPut struct {
	prefix, data, prev []byte
}

func (c *pbinStrictWriteContext) PutBranch(prefix, data, prevData []byte) error {
	if data == nil {
		return fmt.Errorf("pbin test: nil value for %x refused, as the domain would", prefix)
	}
	c.puts = append(c.puts, pbinRecordedPut{bytes.Clone(prefix), bytes.Clone(data), bytes.Clone(prevData)})
	return c.MockState.PutBranch(prefix, data, prevData)
}

func pbinTestStrictEngine(t *testing.T) (*PBinPatriciaHashed, *pbinStrictWriteContext, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	ctx := &pbinStrictWriteContext{MockState: ms}
	return NewPBinPatriciaHashed(ctx), ctx, ms
}

// TestPBinStoreRootEmptiedTreeWritesNonNil pins the empty-root storeRoot path:
// an emptied tree deletes its record by writing a zero-length value, never nil.
func TestPBinStoreRootEmptiedTreeWritesNonNil(t *testing.T) {
	t.Parallel()

	pph, ctx, _ := pbinTestStrictEngine(t)
	require.NoError(t, pph.loadRoot())
	pph.rootTouched = true

	require.NoError(t, pph.storeRoot())
	require.Len(t, ctx.puts, 1)
	put := ctx.puts[0]
	require.Equal(t, pbinRootKey, put.prefix)
	require.NotNil(t, put.data)
	require.Empty(t, put.data)
	require.NotNil(t, put.prev)
}

// TestPBinFoldDeleteWritesNonNilWithRealPrev drives a stored record through the
// touched-but-gone unfold into foldDelete: the deletion write must carry a
// zero-length value, and prevData must be the record bytes the row unfolded
// from — likewise for the root record storeRoot then empties.
func TestPBinFoldDeleteWritesNonNilWithRealPrev(t *testing.T) {
	t.Parallel()

	pph, ctx, ms := pbinTestStrictEngine(t)
	pbinTestPutTopRecord(t, ms, [2]pbinCell{
		pbinTestSpecCell(t, pbinNodeLeaf, "010"),
		pbinTestSpecCell(t, pbinNodeLeaf, "110"),
	})
	emptyPath := pbinBitpath{}
	recordKey := pbinEncodeBitPath(&emptyPath)
	storedRecord := bytes.Clone(ms.cm[string(recordKey)])
	storedRoot := bytes.Clone(ms.cm[string(pbinRootKey)])

	probe := pbinTestPathFromBits(t, pbinTestBitSpec(t, "0101"))
	pbinTestUnfoldStep(t, pph, &probe)
	pph.grid.touchMap[0], pph.grid.afterMap[0] = 0b11, 0
	require.NoError(t, pph.fold())
	require.NoError(t, pph.storeRoot())

	require.Len(t, ctx.puts, 2)
	del, root := ctx.puts[0], ctx.puts[1]
	require.Equal(t, recordKey, del.prefix)
	require.NotNil(t, del.data)
	require.Empty(t, del.data)
	require.Equal(t, storedRecord, del.prev)

	require.Equal(t, pbinRootKey, root.prefix)
	require.NotNil(t, root.data)
	require.Empty(t, root.data)
	require.Equal(t, storedRoot, root.prev)
}

// TestPBinZeroLengthBranchRoundTripsAsDeletion checks the deletion writes all
// the way back around: after the engine empties a stored tree, the zero-length
// records still sitting in the store must read back as no tree at all.
func TestPBinZeroLengthBranchRoundTripsAsDeletion(t *testing.T) {
	t.Parallel()

	pph, _, ms := pbinTestStrictEngine(t)
	pbinTestPutTopRecord(t, ms, [2]pbinCell{
		pbinTestSpecCell(t, pbinNodeLeaf, "010"),
		pbinTestSpecCell(t, pbinNodeLeaf, "110"),
	})

	probe := pbinTestPathFromBits(t, pbinTestBitSpec(t, "0101"))
	pbinTestUnfoldStep(t, pph, &probe)
	pph.grid.touchMap[0], pph.grid.afterMap[0] = 0b11, 0
	require.NoError(t, pph.fold())
	require.NoError(t, pph.storeRoot())

	emptyPath := pbinBitpath{}
	require.Contains(t, ms.cm, string(pbinEncodeBitPath(&emptyPath)))
	require.Contains(t, ms.cm, string(pbinRootKey))

	fresh := NewPBinPatriciaHashed(ms)
	require.NoError(t, fresh.loadRoot())
	require.False(t, fresh.rootPresent, "a zero-length root record must read back as no tree")
	root, err := fresh.RootHash()
	require.NoError(t, err)
	require.Equal(t, make([]byte, length.Hash), root)
}

// pbinRequirePutsMatchStore walks recorded writes in order against what the
// store held before the run, requiring each prevData to be exactly the value
// the write replaces — and non-nil, so the domain never falls back to its own
// read. Returns how many writes replaced an existing record.
func pbinRequirePutsMatchStore(t *testing.T, puts []pbinRecordedPut, store map[string][]byte) (overwrites int) {
	t.Helper()
	for _, put := range puts {
		require.NotNil(t, put.prev, "nil prevData at %x forces an extra domain read", put.prefix)
		require.True(t, bytes.Equal(store[string(put.prefix)], put.prev),
			"prevData at %x does not match the record it replaces", put.prefix)
		if len(put.prev) > 0 {
			overwrites++
		}
		store[string(put.prefix)] = put.data
	}
	return overwrites
}

// TestPBinProcessPutBranchCarriesRealPrev runs a second batch over a stored
// tree and requires every branch write to carry the previous record it
// replaces: empty on a fresh store, the stored bytes on a rewrite.
func TestPBinProcessPutBranchCarriesRealPrev(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(31)
	stored := new(pbinTestCorpus).
		storage(addr, pbinOracleSlot(256), 0x01).
		storage(addr, pbinOracleSlot(257), 0x02).
		account(pbinOracleAddr(32), 3, 4, common.Hash{0x32})
	touch := new(pbinTestCorpus).
		storage(addr, pbinOracleSlot(256), 0x0A).
		storage(addr, pbinOracleSlot(258), 0x03)

	pph, ctx, ms := pbinTestStrictEngine(t)
	require.NoError(t, ms.applyPlainUpdates(stored.plainKeys, stored.updates))
	pbinTestProcess(t, pph, stored.plainKeys, stored.updates)
	require.NotEmpty(t, ctx.puts)
	require.Zero(t, pbinRequirePutsMatchStore(t, ctx.puts, map[string][]byte{}),
		"the first run has nothing to overwrite")

	snapshot := make(map[string][]byte, len(ms.cm))
	for k, v := range ms.cm {
		snapshot[k] = bytes.Clone(v)
	}
	ctx.puts = nil

	require.NoError(t, ms.applyPlainUpdates(touch.plainKeys, touch.updates))
	pph.Reset()
	pbinTestProcess(t, pph, touch.plainKeys, touch.updates)
	require.NotZero(t, pbinRequirePutsMatchStore(t, ctx.puts, snapshot),
		"the second run must rewrite at least one stored record")
}
