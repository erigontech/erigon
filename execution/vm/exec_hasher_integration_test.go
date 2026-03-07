// Copyright 2025 The Erigon Authors
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

package vm_test

import (
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"

	liblog "github.com/erigontech/erigon/common/log/v3"
)

// setupEVM creates a minimal EVM environment for testing contract execution
// with an ExecHasher attached.
func setupEVM(t *testing.T, code []byte) (*vm.EVM, accounts.Address, *vm.ExecHasher) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(t.Context(), tx, liblog.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	txNum, _, err := sd.SeekCommitment(t.Context(), tx)
	require.NoError(t, err)

	r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
	s := state.New(r)

	address := accounts.InternAddress(common.BytesToAddress([]byte("contract")))
	s.CreateAccount(address, true)
	s.SetCode(address, code)

	vmctx := evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
	}
	_ = s.CommitBlock(vmctx.Rules(chain.AllProtocolChanges), w)

	evm := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.AllProtocolChanges, vm.Config{})

	h := vm.GetExecHasher()
	h.Reset()
	evm.SetExecHasher(h)
	t.Cleanup(func() { vm.PutExecHasher(h) })

	return evm, address, h
}

// TestExecHasherEVM_SimpleArithmetic runs PUSH1 2, PUSH1 3, ADD, STOP
// and verifies the execution hash is deterministic.
func TestExecHasherEVM_SimpleArithmetic(t *testing.T) {
	// Bytecode: PUSH1 2, PUSH1 3, ADD, STOP
	code := []byte{0x60, 0x02, 0x60, 0x03, 0x01, 0x00}

	evm1, addr1, h1 := setupEVM(t, code)
	_, _, err := evm1.Call(accounts.ZeroAddress, addr1, nil, math.MaxUint64, uint256.Int{}, false)
	require.NoError(t, err)
	hash1, ops1 := h1.Finalize()

	// Run again - must produce identical hash
	evm2, addr2, h2 := setupEVM(t, code)
	_, _, err = evm2.Call(accounts.ZeroAddress, addr2, nil, math.MaxUint64, uint256.Int{}, false)
	require.NoError(t, err)
	hash2, ops2 := h2.Finalize()

	require.Equal(t, hash1, hash2, "same bytecode must produce identical execution hash")
	require.Equal(t, ops1, ops2)
	require.Equal(t, uint64(4), ops1, "PUSH1 + PUSH1 + ADD + STOP = 4 opcodes")
	t.Logf("execution hash: %x (ops: %d)", hash1, ops1)
}

// TestExecHasherEVM_DifferentBytecode verifies different code produces different hashes.
func TestExecHasherEVM_DifferentBytecode(t *testing.T) {
	// PUSH1 2, PUSH1 3, ADD, STOP
	codeAdd := []byte{0x60, 0x02, 0x60, 0x03, 0x01, 0x00}
	// PUSH1 2, PUSH1 3, MUL, STOP
	codeMul := []byte{0x60, 0x02, 0x60, 0x03, 0x02, 0x00}

	evm1, addr1, h1 := setupEVM(t, codeAdd)
	_, _, err := evm1.Call(accounts.ZeroAddress, addr1, nil, math.MaxUint64, uint256.Int{}, false)
	require.NoError(t, err)
	hashAdd, _ := h1.Finalize()

	evm2, addr2, h2 := setupEVM(t, codeMul)
	_, _, err = evm2.Call(accounts.ZeroAddress, addr2, nil, math.MaxUint64, uint256.Int{}, false)
	require.NoError(t, err)
	hashMul, _ := h2.Finalize()

	require.NotEqual(t, hashAdd, hashMul, "ADD vs MUL must produce different execution hashes")
}

// TestExecHasherEVM_StorageOps tests SSTORE and SLOAD produce hashed execution traces.
func TestExecHasherEVM_StorageOps(t *testing.T) {
	// PUSH1 0x42, PUSH1 0x00, SSTORE, PUSH1 0x00, SLOAD, STOP
	// Store 0x42 at slot 0, then load it back
	code := []byte{
		0x60, 0x42, // PUSH1 0x42
		0x60, 0x00, // PUSH1 0x00
		0x55,       // SSTORE
		0x60, 0x00, // PUSH1 0x00
		0x54,       // SLOAD
		0x00,       // STOP
	}

	evm1, addr1, h1 := setupEVM(t, code)
	_, _, err := evm1.Call(accounts.ZeroAddress, addr1, nil, math.MaxUint64, uint256.Int{}, false)
	require.NoError(t, err)
	hash1, ops1 := h1.Finalize()

	require.Equal(t, uint64(6), ops1, "PUSH1 + PUSH1 + SSTORE + PUSH1 + SLOAD + STOP = 6 ops")

	// Determinism check
	evm2, addr2, h2 := setupEVM(t, code)
	_, _, err = evm2.Call(accounts.ZeroAddress, addr2, nil, math.MaxUint64, uint256.Int{}, false)
	require.NoError(t, err)
	hash2, _ := h2.Finalize()

	require.Equal(t, hash1, hash2)
	t.Logf("storage ops hash: %x (ops: %d)", hash1, ops1)
}

// TestExecHasherEVM_MemoryOps tests MSTORE and MLOAD.
func TestExecHasherEVM_MemoryOps(t *testing.T) {
	// PUSH1 0xFF, PUSH1 0x00, MSTORE, PUSH1 0x00, MLOAD, POP, STOP
	code := []byte{
		0x60, 0xFF, // PUSH1 0xFF
		0x60, 0x00, // PUSH1 0x00
		0x52,       // MSTORE
		0x60, 0x00, // PUSH1 0x00
		0x51,       // MLOAD
		0x50,       // POP
		0x00,       // STOP
	}

	evm, addr, h := setupEVM(t, code)
	_, _, err := evm.Call(accounts.ZeroAddress, addr, nil, math.MaxUint64, uint256.Int{}, false)
	require.NoError(t, err)
	hash, ops := h.Finalize()

	require.Equal(t, uint64(7), ops, "PUSH + PUSH + MSTORE + PUSH + MLOAD + POP + STOP = 7")
	require.NotEqual(t, [32]byte{}, hash)
	t.Logf("memory ops hash: %x (ops: %d)", hash, ops)
}

// TestExecHasherEVM_Revert verifies that reverted execution still produces a hash.
func TestExecHasherEVM_Revert(t *testing.T) {
	// PUSH1 0x00, PUSH1 0x00, REVERT
	code := []byte{
		0x60, 0x00, // PUSH1 0x00 (return size)
		0x60, 0x00, // PUSH1 0x00 (return offset)
		0xFD,       // REVERT
	}

	evm, addr, h := setupEVM(t, code)
	_, _, err := evm.Call(accounts.ZeroAddress, addr, nil, math.MaxUint64, uint256.Int{}, false)
	require.ErrorIs(t, err, vm.ErrExecutionReverted)
	hash, ops := h.Finalize()

	require.Equal(t, uint64(3), ops, "PUSH1 + PUSH1 + REVERT = 3 ops")
	require.NotEqual(t, [32]byte{}, hash, "reverted tx should still produce execution hash")
	t.Logf("revert hash: %x (ops: %d)", hash, ops)
}

// TestExecHasherEVM_NoCode verifies that calling an address with no code
// produces zero ops (EVM.Run is never called).
func TestExecHasherEVM_NoCode(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), tx, liblog.New())
	require.NoError(t, err)
	defer sd.Close()

	txNum, _, err := sd.SeekCommitment(t.Context(), tx)
	require.NoError(t, err)

	r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
	s := state.New(r)

	// Create account with no code
	address := accounts.InternAddress(common.BytesToAddress([]byte("empty")))
	s.CreateAccount(address, true)

	vmctx := evmtypes.BlockContext{
		CanTransfer: func(evmtypes.IntraBlockState, accounts.Address, uint256.Int) (bool, error) { return true, nil },
		Transfer: func(evmtypes.IntraBlockState, accounts.Address, accounts.Address, uint256.Int, bool, *chain.Rules) error {
			return nil
		},
	}
	_ = s.CommitBlock(vmctx.Rules(chain.AllProtocolChanges), w)

	evm := vm.NewEVM(vmctx, evmtypes.TxContext{}, s, chain.AllProtocolChanges, vm.Config{})
	h := vm.GetExecHasher()
	h.Reset()
	evm.SetExecHasher(h)
	defer vm.PutExecHasher(h)

	_, _, err = evm.Call(accounts.ZeroAddress, address, nil, math.MaxUint64, uint256.Int{}, false)
	require.NoError(t, err)
	_, ops := h.Finalize()

	require.Equal(t, uint64(0), ops, "no-code call should hash zero opcodes")
}

// TestExecHasherEVM_Loop tests a tight opcode loop to verify opcode counting.
func TestExecHasherEVM_Loop(t *testing.T) {
	// PUSH1 5, JUMPDEST, PUSH1 1, SWAP1, SUB, DUP1, PUSH1 2, JUMPI, STOP
	// This loops 5 times: decrement counter from 5 to 0
	code := []byte{
		0x60, 0x05, // PUSH1 5 (counter)
		0x5B,       // JUMPDEST (pc=2)
		0x60, 0x01, // PUSH1 1
		0x90,       // SWAP1
		0x03,       // SUB
		0x80,       // DUP1
		0x60, 0x02, // PUSH1 2 (jump target = JUMPDEST)
		0x57,       // JUMPI
		0x00,       // STOP
	}

	evm, addr, h := setupEVM(t, code)
	_, _, err := evm.Call(accounts.ZeroAddress, addr, nil, math.MaxUint64, uint256.Int{}, false)
	require.NoError(t, err)
	hash, ops := h.Finalize()

	// PUSH1 (1) + 5 iterations of (JUMPDEST + PUSH1 + SWAP1 + SUB + DUP1 + PUSH1 + JUMPI) + STOP
	// = 1 + 5*7 + 1 = 37
	// But last iteration: JUMPI doesn't jump (counter=0), falls through to STOP
	// = 1 + 5*(JUMPDEST+PUSH1+SWAP1+SUB+DUP1+PUSH1+JUMPI) + STOP
	// = 1 + 35 + 1 = 37
	require.Greater(t, ops, uint64(30), "loop should execute many opcodes")
	require.NotEqual(t, [32]byte{}, hash)
	t.Logf("loop hash: %x (ops: %d)", hash, ops)
}
