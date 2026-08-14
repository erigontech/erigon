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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// feeMergeRevalidationFixture is a zero-tip tx (txIndex 2) that doesn't touch
// the coinbase, with the coinbase starting EIP-161-empty. mergeFeePass drives
// the same blockExecutor.mergeFeeWrites the apply loop's validate step calls.
type feeMergeRevalidationFixture struct {
	be       *blockExecutor
	result   *execResult
	task     *taskVersion
	reader   *mapStateReader
	rules    *chain.Rules
	coinbase accounts.Address
	version  state.Version
}

func newFeeMergeRevalidationFixture(t *testing.T) *feeMergeRevalidationFixture {
	t.Helper()

	coinbase := fAddr("coinbase")
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000, GasUsed: 21000}
	version := state.Version{BlockNum: 1, TxNum: 3, TxIndex: 2}
	txTask := &exec.TxTask{
		Header: header, TxNum: version.TxNum, TxIndex: version.TxIndex,
		Config:          chain.TestChainBerlinConfig,
		EvmBlockContext: evmtypes.BlockContext{BlockNumber: 1},
	}
	task := &taskVersion{execTask: &execTask{Task: txTask, shouldDelayFeeCalc: true}, version: version}
	result := &execResult{TxResult: &exec.TxResult{
		Task:            task,
		ExecutionResult: evmtypes.ExecutionResult{BurntContractAddress: accounts.NilAddress},
		Coinbase:        coinbase,
	}}

	other := fAddr("other")
	result.TxOut = &state.WriteSet{}
	result.TxOut.SetBalance(other, &state.VersionedWrite[uint256.Int]{
		WriteHeader: state.WriteHeader{Address: other, Path: state.BalancePath, Version: version},
		Val:         *uint256.NewInt(7),
	})

	be := &blockExecutor{
		versionMap:   state.NewVersionMap(nil),
		blockIO:      state.NewVersionedIO(8),
		feeMergeTemp: map[int]*state.WriteSet{},
	}
	be.blockIO.RecordWrites(version, result.TxOut)

	return &feeMergeRevalidationFixture{
		be:       be,
		result:   result,
		task:     task,
		reader:   newMapStateReader(),
		rules:    &chain.Rules{IsSpuriousDragon: true},
		coinbase: coinbase,
		version:  version,
	}
}

// fundCoinbase simulates a lower tx (txIndex 1) re-executing and crediting
// the coinbase, committed to the version map.
func (f *feeMergeRevalidationFixture) fundCoinbase() {
	f.be.versionMap.WriteBalance(f.coinbase, state.Version{TxIndex: 1}, *uint256.NewInt(9_000_000), true)
	f.be.versionMap.WriteAddress(f.coinbase, state.Version{TxIndex: 1}, &accounts.Account{Balance: *uint256.NewInt(9_000_000)}, true)
}

// mergeFeePass runs one apply-loop validation round's fee merge — the same
// calcFees + blockExecutor.mergeFeeWrites + versionMap flush sequence
// exec3_parallel.go's validate loop runs — and returns calcFees' own output
// for the caller to inspect.
func (f *feeMergeRevalidationFixture) mergeFeePass(t *testing.T) *state.WriteSet {
	t.Helper()
	tipWrites, err := f.result.calcFees(f.task, f.be.versionMap, f.reader, f.rules)
	require.NoError(t, err)
	f.be.mergeFeeWrites(0, f.version, f.result.TxOut, tipWrites)
	f.be.versionMap.FlushVersionedWrites(f.be.blockIO.WriteSet(f.version.TxIndex), true, "")
	return tipWrites
}

// mergeFeePassUnflushed is a round that never reaches the flush, which is what
// the validate loop does when a read comes back too early to judge.
func (f *feeMergeRevalidationFixture) mergeFeePassUnflushed(t *testing.T) *state.WriteSet {
	t.Helper()
	tipWrites, err := f.result.calcFees(f.task, f.be.versionMap, f.reader, f.rules)
	require.NoError(t, err)
	f.be.mergeFeeWrites(0, f.version, f.result.TxOut, tipWrites)
	return tipWrites
}

func (f *feeMergeRevalidationFixture) normalize(t *testing.T) *state.WriteSet {
	t.Helper()
	rawWrites := f.be.blockIO.WriteSet(f.version.TxIndex)
	norm, err := rawWrites.Normalize(f.be.versionMap, f.version.TxIndex, f.version.Incarnation,
		f.reader, nil, true /*emptyRemoval*/, false /*isAura*/, false /*eip8246*/)
	require.NoError(t, err)
	return norm
}

// TestFeeMergeRevalidationDropsStaleDelete covers a zero-tip tx that doesn't
// touch the coinbase and first validates in a batch where an earlier tx is
// still invalid and the coinbase is genuinely EIP-161-empty, so calcFees
// emits a SelfDestructPath delete for it. The earlier tx is then fixed and
// funds the coinbase without this tx re-executing, so its second calcFees
// pass emits nothing. The recorded write set must reflect that — not keep
// the first pass's delete merged in — once normalized for commit.
func TestFeeMergeRevalidationDropsStaleDelete(t *testing.T) {
	f := newFeeMergeRevalidationFixture(t)

	pass1 := f.mergeFeePass(t)
	require.False(t, pass1.IsEmpty(), "pass 1 should emit the EIP-161 delete")
	sd1, ok := pass1.GetSelfDestruct(f.coinbase)
	require.True(t, ok)
	require.True(t, sd1.Val)

	f.fundCoinbase()

	pass2 := f.mergeFeePass(t)
	require.True(t, pass2.IsEmpty(), "coinbase is funded now, calcFees has nothing to contribute")

	norm := f.normalize(t)

	_, committed := norm.GetSelfDestruct(f.coinbase)
	require.False(t, committed, "stale EIP-161 delete from pass 1 must not survive a revalidation pass that no longer produces it")

	// This tx never touches the coinbase once the stale delete is dropped —
	// the funded balance is tx 1's own contribution, not this tx's.
	_, hasBal := norm.GetBalance(f.coinbase)
	require.False(t, hasBal, "this tx must stay silent about the coinbase, not fabricate a balance write either")
}

// TestFeeMergeRevalidationRetractsStaleVersionMapCell covers the version-map
// side of the same scenario: FlushVersionedWrites is upsert-only, so pass 1's
// fee cell stays in the version map even after pass 2's merge drops it from
// blockIO. A downstream tx reading through that cell must not see a delete
// this tx no longer commits.
func TestFeeMergeRevalidationRetractsStaleVersionMapCell(t *testing.T) {
	f := newFeeMergeRevalidationFixture(t)

	pass1 := f.mergeFeePass(t)
	require.False(t, pass1.IsEmpty(), "pass 1 should emit the EIP-161 delete")
	_, ok := pass1.GetSelfDestruct(f.coinbase)
	require.True(t, ok)

	_, _, found := f.be.versionMap.ReadSelfDestruct(f.coinbase, f.version.TxIndex+1)
	require.True(t, found, "pass 1's flush must land a cell in the version map")

	f.fundCoinbase()

	pass2 := f.mergeFeePass(t)
	require.True(t, pass2.IsEmpty(), "coinbase is funded now, calcFees has nothing to contribute")

	_, _, found = f.be.versionMap.ReadSelfDestruct(f.coinbase, f.version.TxIndex+1)
	require.False(t, found, "pass 1's stale delete cell must be retracted from the version map once pass 2 no longer produces it")
}

// A round can record a fee contribution and never flush it, so the retraction
// has to tolerate a header the version map never received.
func TestFeeMergeRevalidationRetractsUnflushedHeader(t *testing.T) {
	f := newFeeMergeRevalidationFixture(t)

	pass1 := f.mergeFeePassUnflushed(t)
	_, ok := pass1.GetSelfDestruct(f.coinbase)
	require.True(t, ok, "pass 1 emits the delete but never reaches the flush")

	f.fundCoinbase()

	require.NotPanics(t, func() { f.mergeFeePassUnflushed(t) })
}

// A later round that contributes something else must replace the earlier round
// rather than stack on it: a delete and the credit that supersedes it cannot
// both be the tx's fee contribution.
func TestFeeMergeRevalidationReplacesRatherThanStacks(t *testing.T) {
	f := newFeeMergeRevalidationFixture(t)

	pass1 := f.mergeFeePass(t)
	sd, ok := pass1.GetSelfDestruct(f.coinbase)
	require.True(t, ok)
	require.True(t, sd.Val, "pass 1 emits the delete for an empty coinbase")

	// The tip is no longer zero, so the next round credits the coinbase instead.
	f.result.ExecutionResult.FeeTipped = *uint256.NewInt(500)

	pass2 := f.mergeFeePass(t)
	_, stillDeletes := pass2.GetSelfDestruct(f.coinbase)
	require.False(t, stillDeletes)
	credit, ok := pass2.GetBalance(f.coinbase)
	require.True(t, ok, "pass 2 credits the tip")
	require.Equal(t, uint64(500), credit.Val.Uint64())

	norm := f.normalize(t)

	_, committed := norm.GetSelfDestruct(f.coinbase)
	require.False(t, committed, "the superseded delete must not ride along with the credit")
	bal, hasBal := norm.GetBalance(f.coinbase)
	require.True(t, hasBal)
	require.Equal(t, uint64(500), bal.Val.Uint64())
}

// A block-end or system tx never reaches the fee merge, so finalize is the
// only thing that builds its recorded set. Re-validation re-finalizes without
// re-executing, so a write an earlier round produced must not survive a round
// that no longer produces it — in blockIO or in the version map.
func TestFinalizeMergeReplacesRatherThanStacks(t *testing.T) {
	f := newFeeMergeRevalidationFixture(t)

	round1 := &state.WriteSet{}
	round1.SetSelfDestruct(f.coinbase, &state.VersionedWrite[bool]{
		WriteHeader: state.WriteHeader{Address: f.coinbase, Path: state.SelfDestructPath, Version: f.version},
		Val:         true,
	})
	merged1 := f.be.mergeFinalizeWrites(f.version, f.result.TxOut, round1)
	f.be.versionMap.FlushVersionedWrites(merged1, true, "")

	_, recorded := f.be.blockIO.WriteSet(f.version.TxIndex).GetSelfDestruct(f.coinbase)
	require.True(t, recorded, "round 1's finalize delete is recorded")
	_, _, found := f.be.versionMap.ReadSelfDestruct(f.coinbase, f.version.TxIndex+1)
	require.True(t, found, "and flushed to the version map")

	beneficiary := fAddr("beneficiary")
	round2 := &state.WriteSet{}
	round2.SetBalance(beneficiary, &state.VersionedWrite[uint256.Int]{
		WriteHeader: state.WriteHeader{Address: beneficiary, Path: state.BalancePath, Version: f.version},
		Val:         *uint256.NewInt(11),
	})
	f.be.mergeFinalizeWrites(f.version, f.result.TxOut, round2)

	_, recorded = f.be.blockIO.WriteSet(f.version.TxIndex).GetSelfDestruct(f.coinbase)
	require.False(t, recorded, "round 1's delete must not outlive a round that no longer produces it")
	_, _, found = f.be.versionMap.ReadSelfDestruct(f.coinbase, f.version.TxIndex+1)
	require.False(t, found, "and its version-map cell must be retracted")

	kept, ok := f.be.blockIO.WriteSet(f.version.TxIndex).GetBalance(beneficiary)
	require.True(t, ok, "round 2's own write survives the rebuild")
	require.Equal(t, uint64(11), kept.Val.Uint64())
}

// TestFeeMergeRevalidationControlNormalizeNeedsTheStaleMerge is the negative
// control: with no first pass ever recorded, Normalize must not invent the
// coinbase delete on its own. This isolates the delete in the other test to
// the merge, not to Normalize's own filtering.
func TestFeeMergeRevalidationControlNormalizeNeedsTheStaleMerge(t *testing.T) {
	f := newFeeMergeRevalidationFixture(t)
	f.fundCoinbase()

	pass := f.mergeFeePass(t)
	require.True(t, pass.IsEmpty())

	norm := f.normalize(t)

	_, committed := norm.GetSelfDestruct(f.coinbase)
	require.False(t, committed, "Normalize must not invent the delete when no pass ever produced it")
}
