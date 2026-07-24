// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package vm_test

import (
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

// runInlineOracle executes code through a fresh EVM (inline loop on or off) and
// returns the observable outcome: return data, remaining regular gas, error.
func runInlineOracle(t *testing.T, self accounts.Address, code []byte, noInline bool) ([]byte, uint64, error) {
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

	vmctx := evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
	}
	vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.AllProtocolChanges, vm.Config{NoInlineDispatch: noInline})
	pool := mdgas.MdGas{Regular: 1_000_000, State: 1_000_000}
	ret, remaining, _, err := vmenv.Call(accounts.ZeroAddress, self, nil, pool, uint256.Int{}, false)
	return ret, remaining.Regular, err
}

// TestInlineDispatch_EquivalenceOracle pins the inline fast loop to the
// jump-table dispatch (the root of trust): for every program the two paths must
// return identical return data, remaining gas, and error. The corpus mixes the
// inlined ops (POP/PUSH1/DUP/SWAP/JUMPDEST/STOP) with non-inlined ops and the
// stack/gas edges (underflow, overflow, OOG, run-off-end).
func TestInlineDispatch_EquivalenceOracle(t *testing.T) {
	t.Parallel()
	self := accounts.InternAddress(common.BytesToAddress([]byte("self")))
	corpus := map[string]string{
		"add_dup_swap_pop_return": "6001600201808050" + "60005260206000f3", // PUSH1 1;PUSH1 2;ADD;DUP1;SWAP1;POP;MSTORE;RETURN
		"stop_only":               "00",
		"push1_run_return":        "6001600260036004505050" + "60005260206000f3",
		"dup16_swap16":            "60016002600360046005600660076008600960016001600b600c600d600e600f" + "8f9f50" + "00",
		"pop_underflow":           "50",     // POP empty -> underflow
		"swap_underflow":          "600190", // PUSH1 1; SWAP1 -> underflow (needs 2)
		"jumpdest_run":            "5b5b5b6001505b00",
		"push1_then_sstore":       "6001600055" + "00", // inlined pushes then a non-inlined SSTORE
		"deep_dup_pop":            "6001" + "8080808080808050505050505050" + "00",
		"run_off_end_no_stop":     "6001600280", // ends mid-stack, no STOP -> implicit halt
	}
	for name, hexProg := range corpus {
		name, hexProg := name, hexProg
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			code := hexcode(t, hexProg)
			retOn, gasOn, errOn := runInlineOracle(t, self, code, false)
			retOff, gasOff, errOff := runInlineOracle(t, self, code, true)
			require.Equal(t, retOff, retOn, "return data must match jump-table dispatch")
			require.Equal(t, gasOff, gasOn, "remaining gas must match jump-table dispatch")
			if errOff == nil {
				require.NoError(t, errOn, "error must match jump-table dispatch (oracle: nil)")
			} else {
				require.EqualError(t, errOn, errOff.Error(), "error must match jump-table dispatch")
			}
		})
	}
}

// benchRunVersionedCfg builds a versioned IBS + EVM (with cfg) once and returns
// a closure that invokes the contract, so the benchmark times execution only.
func benchRunVersionedCfg(b *testing.B, self accounts.Address, code []byte, cfg vm.Config) func() {
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
	vmenv := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.AllProtocolChanges, cfg)
	return func() {
		pool := mdgas.MdGas{Regular: 100_000_000, State: 100_000_000}
		_, _, _, _ = vmenv.Call(accounts.ZeroAddress, self, nil, pool, uint256.Int{}, false)
	}
}

// BenchmarkInlineDispatch measures the separate inline loop vs the jump-table
// dispatch on a PUSH/DUP/SWAP/POP-heavy loop (the cheap-op profile that
// dominates interp-bound txs).
func BenchmarkInlineDispatch(b *testing.B) {
	self := accounts.InternAddress(common.BytesToAddress([]byte("self")))
	var buf []byte
	buf = append(buf, 0x60, 0x01) // PUSH1 1
	for i := 0; i < 256; i++ {
		buf = append(buf, 0x60, 0x02, 0x80, 0x81, 0x90, 0x50, 0x50) // PUSH1 2; DUP1; DUP2; SWAP1; POP; POP
	}
	buf = append(buf, 0x00) // STOP
	for _, tc := range []struct {
		name     string
		noInline bool
	}{{"inline_on", false}, {"inline_off", true}} {
		b.Run(tc.name, func(b *testing.B) {
			run := benchRunVersionedCfg(b, self, buf, vm.Config{NoInlineDispatch: tc.noInline})
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run()
			}
		})
	}
}
