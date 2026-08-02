// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package vm_test

import (
	"encoding/hex"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// These tests are the correctness net for the frame-local SLOAD slot cache
// (parallel/versioned read path). They assert observable SLOAD results through
// real bytecode, so they pass with or without the cache — and a naive cache that
// skips write/CALL invalidation turns the reconcile scenario RED. They must all
// stay green after the cache lands.

// runVersioned deploys code at `self` (and optionally `lib`), then calls `self`
// through a versioned IBS (the parallel read path where the slot cache applies),
// returning the 32-byte result word.
func runVersioned(t *testing.T, self accounts.Address, code []byte, lib accounts.Address, libCode []byte) []byte {
	t.Helper()
	tx, sd := testTemporalTxSD(t)
	_, _, err := sd.SeekCommitment(t.Context(), tx)
	require.NoError(t, err)
	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.NewWithVersionMap(r, state.NewVersionMap(nil))
	s.SetVersion(0)
	defer s.Release(false)

	s.CreateAccount(self, true)
	s.SetCode(self, code, tracing.CodeChangeUnspecified)
	if libCode != nil {
		s.CreateAccount(lib, true)
		s.SetCode(lib, libCode, tracing.CodeChangeUnspecified)
	}

	vmctx := evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
	}
	vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.AllProtocolChanges, vm.Config{})
	pool := mdgas.MdGas{Regular: 10_000_000, State: 10_000_000}
	ret, _, _, err := vmenv.Call(accounts.ZeroAddress, self, nil, pool, uint256.Int{}, false)
	require.NoError(t, err)
	require.Len(t, ret, 32)
	return ret
}

// runVersionedFunded is runVersioned with a real value-transfer callback and a
// funded caller, so a value CALL observably moves balance (the stub Transfer in
// runVersioned is a no-op). Used by the balance-cache reconcile teeth.
func runVersionedFunded(t *testing.T, self accounts.Address, code []byte, fund uint256.Int) []byte {
	t.Helper()
	tx, sd := testTemporalTxSD(t)
	_, _, err := sd.SeekCommitment(t.Context(), tx)
	require.NoError(t, err)
	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.NewWithVersionMap(r, state.NewVersionMap(nil))
	s.SetVersion(0)
	defer s.Release(false)

	s.CreateAccount(self, true)
	s.SetCode(self, code, tracing.CodeChangeUnspecified)
	require.NoError(t, s.AddBalance(self, fund, tracing.BalanceChangeUnspecified))

	vmctx := evmtypes.BlockContext{
		CanTransfer: func(_ evmtypes.IntraBlockState, addr accounts.Address, amount uint256.Int) (bool, error) {
			b, err := s.GetBalance(addr)
			return b.Cmp(&amount) >= 0, err
		},
		Transfer: func(_ evmtypes.IntraBlockState, from, to accounts.Address, amount uint256.Int, _ bool, _ *chain.Rules) error {
			if err := s.SubBalance(from, amount, tracing.BalanceChangeTransfer); err != nil {
				return err
			}
			return s.AddBalance(to, amount, tracing.BalanceChangeTransfer)
		},
	}
	vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.AllProtocolChanges, vm.Config{})
	pool := mdgas.MdGas{Regular: 10_000_000, State: 10_000_000}
	ret, _, _, err := vmenv.Call(accounts.ZeroAddress, self, nil, pool, uint256.Int{}, false)
	require.NoError(t, err)
	require.Len(t, ret, 32)
	return ret
}

func hexcode(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// benchRunVersioned is runVersioned's setup hoisted out of the timed loop: it
// builds the versioned IBS + EVM once and returns a closure that invokes the
// contract, so the benchmark measures execution, not state setup.
func benchRunVersioned(b *testing.B, self accounts.Address, code []byte) func() {
	tx, sd := testTemporalTxSD(b)
	_, _, err := sd.SeekCommitment(b.Context(), tx)
	require.NoError(b, err)
	r := state.NewReaderV3(sd.AsGetter(tx))
	s := state.NewWithVersionMap(r, state.NewVersionMap(nil))
	s.SetVersion(0)
	s.CreateAccount(self, true)
	s.SetCode(self, code, tracing.CodeChangeUnspecified)
	vmctx := evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
	}
	vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.AllProtocolChanges, vm.Config{})
	return func() {
		pool := mdgas.MdGas{Regular: 100_000_000, State: 100_000_000}
		_, _, _, _ = vmenv.Call(accounts.ZeroAddress, self, nil, pool, uint256.Int{}, false)
	}
}

// BenchmarkFrameSlotCache_RepeatedSload is the cache's best case: 64 SLOADs of
// the same slot in one frame. Shows the ceiling of what the frame slot cache
// saves on the versioned read path.
func BenchmarkFrameSlotCache_RepeatedSload(b *testing.B) {
	self := accounts.InternAddress(common.BytesToAddress([]byte("self")))
	var buf []byte
	for i := 0; i < 64; i++ {
		buf = append(buf, 0x60, 0x00, 0x54, 0x50) // PUSH1 0; SLOAD; POP
	}
	buf = append(buf, 0x60, 0x00, 0x60, 0x00, 0xf3) // PUSH1 0; PUSH1 0; RETURN (empty)
	run := benchRunVersioned(b, self, buf)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run()
	}
}

// The account-field cache reconcile teeth: caller reads BALANCE(target) (caching
// 0), then CALLs target sending value 5; the caller's next BALANCE(target) must
// see 5, not the stale cached 0.
func TestFrameBalanceCache_ReconcileAfterCall(t *testing.T) {
	t.Parallel()
	self := accounts.InternAddress(common.BytesToAddress([]byte("self")))
	target := accounts.InternAddress(common.BytesToAddress([]byte("target")))
	tgt := target.Value()

	// 73 <target> 31 50                     PUSH20 target; BALANCE; POP   (cache 0)
	// 6000 6000 6000 6000                    retSize,retOff,inSize,inOff = 0
	// 6005                                    value = 5
	// 73 <target>                             addr = target
	// 5a f1 50                                GAS; CALL; POP
	// 73 <target> 31                          PUSH20 target; BALANCE       (-> 5)
	// 6000 52 6020 6000 f3                     MSTORE; RETURN
	code := hexcode(t, "73"+hex.EncodeToString(tgt[:])+"3150"+
		"600060006000600060"+"05"+
		"73"+hex.EncodeToString(tgt[:])+"5af150"+
		"73"+hex.EncodeToString(tgt[:])+"31"+
		"60005260206000f3")
	ret := runVersionedFunded(t, self, code, *uint256.NewInt(1000))
	require.Equal(t, byte(5), ret[31], "BALANCE after a value CALL must reconcile, not serve a stale frame cache")
}

// SSTORE then two SLOADs of the same slot in one frame must both see the written
// value (a repeated read must not go stale, and must reflect this frame's write).
func TestFrameSlotCache_ReadAfterWriteSameFrame(t *testing.T) {
	t.Parallel()
	self := accounts.InternAddress(common.BytesToAddress([]byte("self")))
	// 602a 6000 55  SSTORE 42@0
	// 6000 54 50    SLOAD 0; POP   (first read)
	// 6000 54       SLOAD 0        (second read -> must be 42)
	// 6000 52       MSTORE
	// 6020 6000 f3  RETURN
	code := hexcode(t, "602a6000556000545060005460005260206000f3")
	ret := runVersioned(t, self, code, accounts.ZeroAddress, nil)
	require.Equal(t, byte(42), ret[31], "second SLOAD must see the value written this frame")
}

// SLOAD (cache), then SSTORE a new value, then SLOAD again: the second read must
// reflect the write, not a stale cached value.
func TestFrameSlotCache_ReadThenWriteThenRead(t *testing.T) {
	t.Parallel()
	self := accounts.InternAddress(common.BytesToAddress([]byte("self")))
	// 6000 54 50    SLOAD 0; POP     (=0, cache)
	// 6007 6000 55  SSTORE 7@0
	// 6000 54       SLOAD 0          (-> must be 7)
	// 6000 52 6020 6000 f3
	code := hexcode(t, "60005450600760005560005460005260206000f3")
	ret := runVersioned(t, self, code, accounts.ZeroAddress, nil)
	require.Equal(t, byte(7), ret[31], "SLOAD after SSTORE must see the new value")
}

// The reconcile teeth: caller SLOADs slot 0 (populating a frame cache), then
// DELEGATECALLs a lib that SSTOREs slot 0 = 99 in the caller's storage context;
// the caller's next SLOAD must see 99, not the stale cached 0.
func TestFrameSlotCache_DelegateCallWriteReconcile(t *testing.T) {
	t.Parallel()
	self := accounts.InternAddress(common.BytesToAddress([]byte("self")))
	lib := accounts.InternAddress(common.BytesToAddress([]byte("lib")))
	libVal := lib.Value()

	// lib: 6063 6000 55 00   SSTORE 99@0; STOP
	libCode := hexcode(t, "606360005500")

	// caller:
	//   6000 54 50                         SLOAD 0; POP        (cache 0)
	//   6000 6000 6000 6000                retLen,retOff,argLen,argOff = 0
	//   73 <lib 20 bytes>                  PUSH20 lib
	//   5a                                 GAS
	//   f4                                 DELEGATECALL
	//   50                                 POP (success flag)
	//   6000 54                            SLOAD 0             (-> must be 99)
	//   6000 52 6020 6000 f3               MSTORE; RETURN
	code := hexcode(t, "600054506000600060006000"+"73"+hex.EncodeToString(libVal[:])+"5af450"+"60005460005260206000f3")
	ret := runVersioned(t, self, code, lib, libCode)
	require.Equal(t, byte(99), ret[31], "SLOAD after a DELEGATECALL write must reconcile, not serve a stale frame cache")
}
