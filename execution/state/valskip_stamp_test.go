package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestValSkip_SelfDestructPromotionBumpsStamp(t *testing.T) {
	t.Parallel()
	vm := NewVersionMap(nil)
	vm.InitStamps(32)

	addr := accounts.InternAddress(common.HexToAddress("0x18b2b7673c6d661923e9460d592699617828b293"))
	slot := accounts.InternKey(common.HexToHash("0x08"))
	val := *uint256.NewInt(0x1111)
	const k, j, i = 1, 3, 5

	vm.WriteStorage(addr, slot, Version{TxIndex: k}, val, true)

	sd := &WriteSet{}
	sd.SetSelfDestruct(addr, &VersionedWrite[bool]{
		WriteHeader: WriteHeader{Address: addr, Version: Version{TxIndex: j}, Path: SelfDestructPath},
		Val:         true,
	})
	vm.FlushVersionedWrites(sd, false)

	rs := ReadSet{}
	rs.SetStorage(addr, slot, VersionedRead[uint256.Int]{
		ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: k}},
		Val:        val,
	})
	check := func(rv, wv Version) VersionValidity {
		if rv != wv {
			return VersionInvalid
		}
		return VersionValid
	}

	stamp := vm.WriteSeq()
	require.Equal(t, VersionValid, vm.ValidateReadSet(i, rs, check, false, ""),
		"worker-side validation passes while the destruct cell is Estimate")
	require.True(t, vm.QuietBelow(i, stamp), "nothing changed since the stamp")

	vm.MarkWritesValidated(sd, nil)

	require.NotEqual(t, VersionValid, vm.ValidateReadSet(i, rs, check, false, ""),
		"after the destruct is Validated, the read of the destroyed contract's slot is stale")
	require.False(t, vm.QuietBelow(i, stamp),
		"promoting a lifecycle cell must invalidate stamps taken before it")
}

func newStampedMap(n int) *VersionMap {
	vm := NewVersionMap(nil)
	vm.InitStamps(n)
	return vm
}

func TestQuietBelow_InFlightWriteBelowBlocks(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	stamp := vm.WriteSeq()
	vm.mutBegin(3)
	require.False(t, vm.QuietBelow(5, stamp), "a write in flight below i must block")
	vm.mutEnd(3)
}

func TestQuietBelow_NewerCompletedWriteBelowBlocks(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	stamp := vm.WriteSeq()
	require.True(t, vm.QuietBelow(5, stamp))
	vm.mutBegin(3)
	vm.mutEnd(3)
	require.False(t, vm.QuietBelow(5, stamp), "a write completed after the stamp must block")
	require.True(t, vm.QuietBelow(5, vm.WriteSeq()), "a stamp taken after it must not")
}

func TestQuietBelow_WriteAtOrAboveDoesNotBlock(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	stamp := vm.WriteSeq()
	for _, j := range []int{5, 6, 20, 31} {
		vm.mutBegin(j)
		require.True(t, vm.QuietBelow(5, stamp), "in-flight write at j=%d >= i must not block", j)
		vm.mutEnd(j)
		require.True(t, vm.QuietBelow(5, stamp), "completed write at j=%d >= i must not block", j)
	}
}

func TestQuietBelow_AdjacentBoundary(t *testing.T) {
	t.Parallel()
	const i = 5
	vm := newStampedMap(32)
	stamp := vm.WriteSeq()
	vm.mutBegin(i - 1)
	require.False(t, vm.QuietBelow(i, stamp), "in-flight write at j=i-1 must block")
	vm.mutEnd(i - 1)
	require.False(t, vm.QuietBelow(i, stamp), "completed write at j=i-1 must block")

	vm = newStampedMap(32)
	stamp = vm.WriteSeq()
	vm.mutBegin(i)
	vm.mutEnd(i)
	require.True(t, vm.QuietBelow(i, stamp), "write at j=i must not block")
}

func TestQuietBelow_SystemTxWriterBlocks(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	stamp := vm.WriteSeq()
	vm.mutBegin(-1)
	require.False(t, vm.QuietBelow(0, stamp), "in-flight system-tx write must block tx 0")
	require.False(t, vm.QuietBelow(7, stamp), "in-flight system-tx write must block tx 7")
	vm.mutEnd(-1)
	require.False(t, vm.QuietBelow(0, stamp), "completed system-tx write must block tx 0")
	require.True(t, vm.QuietBelow(0, vm.WriteSeq()))
}

func TestQuietBelow_ZeroStampOrNoStampsNeverQuiet(t *testing.T) {
	t.Parallel()
	require.False(t, NewVersionMap(nil).QuietBelow(5, 1), "uninitialised stamps")
	vm := newStampedMap(32)
	require.False(t, vm.QuietBelow(5, 0), "zero stamp means not taken")
}

func TestQuietBelow_FlushBumpsStampOfWriterSlot(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	addr := accounts.InternAddress(common.HexToAddress("0x01"))
	stamp := vm.WriteSeq()
	ws := &WriteSet{}
	ws.SetBalance(addr, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Version: Version{TxIndex: 2}, Path: BalancePath},
		Val:         *uint256.NewInt(1),
	})
	vm.FlushVersionedWrites(ws, false)
	require.False(t, vm.QuietBelow(3, stamp), "flush by tx 2 blocks tx 3")
	require.True(t, vm.QuietBelow(2, stamp), "flush by tx 2 does not block tx 2")
}

type cellKind int

const (
	cellIncarnation cellKind = iota
	cellCreateContract
	cellSelfDestruct
	cellBalance
)

func flushLifecycle(vm *VersionMap, addr accounts.Address, tx int, kind cellKind) *WriteSet {
	ws := &WriteSet{}
	h := WriteHeader{Address: addr, Version: Version{TxIndex: tx}}
	switch kind {
	case cellIncarnation:
		h.Path = IncarnationPath
		ws.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: h, Val: 1})
	case cellCreateContract:
		h.Path = CreateContractPath
		ws.SetCreateContract(addr, &VersionedWrite[bool]{WriteHeader: h, Val: true})
	case cellSelfDestruct:
		h.Path = SelfDestructPath
		ws.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: h, Val: true})
	case cellBalance:
		h.Path = BalancePath
		ws.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: h, Val: *uint256.NewInt(1)})
	}
	vm.FlushVersionedWrites(ws, false)
	return ws
}

func TestValSkip_LifecyclePromotionBumpsStamp(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x02"))
	for name, kind := range map[string]cellKind{
		"incarnation":    cellIncarnation,
		"createContract": cellCreateContract,
		"selfDestruct":   cellSelfDestruct,
	} {
		t.Run(name, func(t *testing.T) {
			vm := newStampedMap(32)
			ws := flushLifecycle(vm, addr, 3, kind)
			stamp := vm.WriteSeq()
			require.True(t, vm.QuietBelow(5, stamp))
			vm.MarkWritesValidated(ws, nil)
			require.False(t, vm.QuietBelow(5, stamp), "promotion of a %s cell must bump", name)
		})
	}
}

func TestValSkip_CompleteOnLifecycleCellBumps(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	addr := accounts.InternAddress(common.HexToAddress("0x03"))
	ws := flushLifecycle(vm, addr, 3, cellSelfDestruct)
	vm.MarkWritesValidated(ws, nil)
	stamp := vm.WriteSeq()
	require.True(t, vm.QuietBelow(5, stamp))
	vm.MarkWritesComplete(ws)
	require.False(t, vm.QuietBelow(5, stamp), "Validated->Done on a SelfDestruct cell must bump")
}

func TestValSkip_PromotionWithoutLifecycleCellDoesNotBump(t *testing.T) {
	t.Parallel()
	vm := newStampedMap(32)
	addr := accounts.InternAddress(common.HexToAddress("0x04"))
	ws := flushLifecycle(vm, addr, 3, cellBalance)
	stamp := vm.WriteSeq()
	vm.MarkWritesValidated(ws, nil)
	require.True(t, vm.QuietBelow(5, stamp), "Estimate->Validated on a plain account must not bump")
	vm.MarkWritesComplete(ws)
	require.True(t, vm.QuietBelow(5, stamp), "Validated->Done on a plain account must not bump")
	require.Zero(t, vm.stamps.inflight[vm.stamps.slot(3)+1].Load(), "cancelled brackets leave no in-flight count")
}

func TestQuietBelow_DeleteBumpsStamp(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x05"))
	for _, tc := range []struct {
		j     int
		quiet bool
	}{{3, false}, {4, false}, {5, true}, {9, true}} {
		vm := newStampedMap(32)
		flushLifecycle(vm, addr, tc.j, cellBalance)
		stamp := vm.WriteSeq()
		vm.Delete(addr, BalancePath, accounts.NilKey, tc.j, true)
		require.Equal(t, tc.quiet, vm.QuietBelow(5, stamp), "Delete by j=%d against i=5", tc.j)
	}
}

func TestQuietBelow_DeleteAllBumpsStamp(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x06"))
	for _, tc := range []struct {
		j     int
		quiet bool
	}{{3, false}, {4, false}, {5, true}, {9, true}} {
		vm := newStampedMap(32)
		flushLifecycle(vm, addr, tc.j, cellBalance)
		stamp := vm.WriteSeq()
		vm.DeleteAll(addr, tc.j)
		require.Equal(t, tc.quiet, vm.QuietBelow(5, stamp), "DeleteAll by j=%d against i=5", tc.j)
	}
}
