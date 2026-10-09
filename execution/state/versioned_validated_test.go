package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// The Validated 3-state optimization splits into two independent invariants,
// kept as separate tests so a failure localizes to exactly one:
//
//   - PAUSE (perf): a write a tx validated out-of-order but has not yet sealed is
//     Validated, not Done. A later reader EARLY-BREAKS on it — reads the value and
//     continues, the same action as a Done read — instead of parking on
//     MVReadResultDependency the way a plain Estimate forces.
//   - SEAL-TIME MATCH (correctness): the early-break is safe only because, at the
//     reader's own seal, its recorded read-dep is confirmed against the final
//     (sealed) value. A reader whose recorded read no longer matches the final dep
//     is invalid and must re-execute before it can seal.

func validatedTestReader(t *testing.T) (*VersionMap, StateReader) {
	t.Helper()
	_, tx, domains := NewTestRwTx(t)
	vm := NewVersionMap(nil)
	reader := NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	return vm, reader
}

// writeValidated flushes a single storage write as Estimate at (txIndex,0) then
// promotes it Estimate->Validated, returning the write-set so callers can seal it.
func writeValidated(t *testing.T, vm *VersionMap, reader StateReader, txIndex int, addr accounts.Address, key accounts.StorageKey, val uint256.Int) *WriteSet {
	t.Helper()
	w := NewWithVersionMap(reader, vm)
	t.Cleanup(w.Close)
	w.txIndex = txIndex
	require.NoError(t, w.SetState(addr, key, val))
	writes := w.VersionedWrites()
	vm.FlushVersionedWrites(writes, false, "")
	vm.MarkWritesValidated(writes, nil)
	return writes
}

// TestValidated_EarlyBreakContinuesInsteadOfPausing pins the PAUSE invariant: a
// reader that finds a Validated dependency continues (reads the value) rather than
// parking, which is what a plain Estimate would force.
func TestValidated_EarlyBreakContinuesInsteadOfPausing(t *testing.T) {
	t.Parallel()
	vm, reader := validatedTestReader(t)

	addr := accounts.InternAddress(common.HexToAddress("0x18b2b7673c6d661923e9460d592699617828b293"))
	slot := accounts.InternKey(common.HexToHash("0x08"))
	v1 := *uint256.NewInt(0x1111)

	writeValidated(t, vm, reader, 3, addr, slot, v1)

	val, res, ok := vm.ReadStorage(addr, slot, 16)
	require.True(t, ok)
	require.Equal(t, v1, val, "early-break reader proceeds on the Validated value")
	require.Equal(t, MVReadResultValidated, res.Status(),
		"a Validated cell must be early-breakable (continue), not a park (MVReadResultDependency)")
}

// TestValidated_SealTimeReadDepMustMatchFinalDep pins the SEAL-TIME MATCH
// invariant: a reader that early-broke on a Validated value is invalid at seal
// when its recorded read-dep no longer matches the final (sealed) value, so the
// executor re-executes it instead of committing the stale read.
func TestValidated_SealTimeReadDepMustMatchFinalDep(t *testing.T) {
	t.Parallel()
	vm, reader := validatedTestReader(t)

	addr := accounts.InternAddress(common.HexToAddress("0x18b2b7673c6d661923e9460d592699617828b293"))
	slot := accounts.InternKey(common.HexToHash("0x08"))
	v1 := *uint256.NewInt(0x1111)
	v2 := *uint256.NewInt(0x2222)

	writeValidated(t, vm, reader, 3, addr, slot, v1)

	// Reader early-breaks on the Validated value and records the dependency.
	r := NewWithVersionMap(reader, vm)
	defer r.Close()
	r.txIndex = 16
	got, err := r.GetState(addr, slot)
	require.NoError(t, err)
	require.Equal(t, v1, got)

	// The writer re-executes, re-resolves the slot, and seals it as the final
	// value at a new incarnation.
	vm.WriteStorage(addr, slot, Version{TxIndex: 3, Incarnation: 1}, v2, true)

	valid := vm.ValidateReadSet(16, r.VersionedReads(),
		func(rv, wv Version) VersionValidity {
			if rv != wv {
				return VersionInvalid
			}
			return VersionValid
		}, false, "")
	require.Equal(t, VersionInvalid, valid,
		"read-dep (v1) must not match the final sealed dep (v2) at seal -> reader re-executes")
}

// TestValidated_SealSameIncarnationStaysValid pins the complement of the seal-time
// mismatch: when the writer never re-executes, promoting its cell Validated->Done
// preserves the incarnation, so an early-break reader's recorded dep still matches
// the final dep and the reader is NOT re-executed. Without this, every early-break
// reader would re-run and erase the perf win.
func TestValidated_SealSameIncarnationStaysValid(t *testing.T) {
	t.Parallel()
	vm, reader := validatedTestReader(t)

	addr := accounts.InternAddress(common.HexToAddress("0x18b2b7673c6d661923e9460d592699617828b293"))
	slot := accounts.InternKey(common.HexToHash("0x08"))
	v1 := *uint256.NewInt(0x1111)

	wWrites := writeValidated(t, vm, reader, 3, addr, slot, v1)

	r := NewWithVersionMap(reader, vm)
	defer r.Close()
	r.txIndex = 16
	got, err := r.GetState(addr, slot)
	require.NoError(t, err)
	require.Equal(t, v1, got)

	// The writer seals with no re-execution: Validated -> Done at the same
	// incarnation, same value.
	vm.MarkWritesComplete(wWrites)

	valid := vm.ValidateReadSet(16, r.VersionedReads(),
		func(rv, wv Version) VersionValidity {
			if rv != wv {
				return VersionInvalid
			}
			return VersionValid
		}, false, "")
	require.Equal(t, VersionValid, valid,
		"read-dep matches the final sealed dep at the same incarnation -> reader stays valid, no re-exec")
}
