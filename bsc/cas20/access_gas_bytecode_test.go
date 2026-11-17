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

package cas20

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/vm"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
)

// The native caller stays at Self while its target is another account. The
// reference executes vm.EXTCODEHASH at Self, or vm.SLOAD in the target's context.
type cas20AccessProgram struct {
	cas20StatefulBase
	account bool
	target  common.Address
	slot    common.Hash
	repeats int
}

func (p cas20AccessProgram) Name() string { return "CAS20 access test" }

func (p cas20AccessProgram) bytecode() []byte {
	var code []byte
	for i := 0; i < p.repeats; i++ {
		if p.account {
			code = append(code, byte(vm.PUSH20))
			code = append(code, p.target[:]...)
			code = append(code, byte(vm.EXTCODEHASH))
		} else {
			code = append(code, byte(vm.PUSH32))
			code = append(code, p.slot[:]...)
			code = append(code, byte(vm.SLOAD))
		}
		code = append(code, byte(vm.POP))
	}
	return code
}

func (p cas20AccessProgram) RunStateful(ctx *PrecompileContext, _ []byte) ([]byte, error) {
	var values []byte
	for i := 0; i < p.repeats; i++ {
		// Only stack scaffolding is priced here. The production access helpers
		// supply all account/storage charges and access-list changes.
		if !ctx.chargeGas(vm.GasFastestStep) {
			return nil, vm.ErrOutOfGas
		}
		var value common.Hash
		if p.account {
			if !ctx.chargeAccountAccess(p.target) {
				return nil, vm.ErrOutOfGas
			}
			value = ctx.StateDB.GetCodeHash(p.target)
		} else {
			var ok bool
			value, ok = newMeteredCAS20StorageAt(ctx, p.target).getWordChecked(p.slot)
			if !ok {
				return nil, vm.ErrOutOfGas
			}
		}
		if !ctx.chargeGas(vm.GasQuickStep) {
			return nil, vm.ErrOutOfGas
		}
		// Expose native reads to the test so using Self's slot cannot pass just
		// because both slots happen to have the same access cost.
		values = append(values, value[:]...)
	}
	return finishCAS20Metered(ctx, values, nil)
}

func TestCAS20AccessGasAgainstBytecode(t *testing.T) {
	self := common.HexToAddress("0x1234")
	slot := common.Hash{31: 1}
	for _, target := range []struct {
		name    string
		address common.Address
		account bool
	}{
		{"account", common.HexToAddress("0x5678"), true},
		{"policy-registry", CAS20PolicyRegistryAddress, false},
		{"activation-registry", CAS20ActivationRegistryAddress, false},
	} {
		for _, warm := range []struct {
			name     string
			account  bool
			slot     bool
			selfSlot bool
		}{
			{"cold", false, false, false},
			{"account-warm", true, false, false},
			{"slot-warm", true, true, false},
			{"self-slot-warm", false, false, true},
		} {
			if target.account && (warm.slot || warm.selfSlot) {
				continue
			}
			for _, repeats := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/%s/reads=%d", target.name, warm.name, repeats), func(t *testing.T) {
					program := cas20AccessProgram{account: target.account, target: target.address, slot: slot, repeats: repeats}
					setup := func() *testState {
						initial := newTestStateDB(t)
						initial.SetCode(self, program.bytecode(), tracing.CodeChangeContractCreation)
						initial.SetCode(target.address, program.bytecode(), tracing.CodeChangeContractCreation)
						initial.SetState(self, slot, common.Hash{31: 0x11})
						initial.SetState(target.address, slot, common.Hash{31: 0x22})
						initial.AddAddressToAccessList(self)
						if warm.account {
							initial.AddAddressToAccessList(target.address)
						}
						if warm.slot {
							initial.AddSlotToAccessList(target.address, slot)
						}
						if warm.selfSlot {
							initial.AddSlotToAccessList(self, slot)
						}
						return initial
					}
					initial := setup()
					root := setup().IntermediateRoot(true)
					value := initial.GetState(target.address, slot)
					if target.account {
						// The target has code, so GetCodeHash and vm.EXTCODEHASH agree
						// without involving their different empty-account semantics.
						value = initial.GetCodeHash(target.address)
					}
					expected := bytes.Repeat(value[:], repeats)
					type result struct {
						gas    mdgas.MdGas
						access [4]bool
						root   common.Hash
						refund uint64
						output []byte
						err    error
					}
					run := func(native bool, budget uint64) result {
						sdb := setup()
						callee := self
						cfg := cas20TestChainConfig()
						if !native {
							if !target.account {
								callee = target.address
							}
							// Run the oracle bytecode even at a reserved registry
							// address. EVM.Call adds no CALL opcode/account fee;
							// only vm.SLOAD's own storage charge is being compared.
							cfg.JennerTime = nil
						}
						evm := newTestEVM(cas20BlockContext(1), sdb, cfg)
						if native {
							evm.SetPrecompiles(map[common.Address]cas20Precompile{self: program})
						}
						out, left, err := evm.Call(cas20Alice, callee, nil, NewGasBudget(budget), new(uint256.Int))
						selfAddr, selfSlot := sdb.SlotInAccessList(self, slot)
						targetAddr, targetSlot := sdb.SlotInAccessList(target.address, slot)
						refund := sdb.GetRefund()
						return result{left, [4]bool{selfAddr, selfSlot, targetAddr, targetSlot}, sdb.IntermediateRoot(true), refund, out, err}
					}
					const ample = uint64(100_000)
					oracle := run(false, ample)
					if oracle.err != nil {
						t.Fatal(oracle.err)
					}
					used := ample - oracle.gas.Execution
					// Exact gas succeeds. One less fails at vm.POP; three less fails
					// during the last access; three gas fails at the first access.
					for _, budget := range []uint64{ample, used, used - 1, used - 3, 3} {
						t.Run(fmt.Sprintf("gas=%d", budget), func(t *testing.T) {
							want, got := run(false, budget), run(true, budget)
							for _, r := range []result{want, got} {
								if r.err != nil && !errors.Is(r.err, vm.ErrOutOfGas) {
									t.Fatalf("unexpected execution error: %v", r.err)
								}
								if (r.err == nil) != (budget >= used) || r.root != root || r.refund != 0 {
									t.Fatalf("unexpected result: gas=%+v root=%s refund=%d err=%v", r.gas, r.root, r.refund, r.err)
								}
							}
							if got.gas != want.gas || got.access != want.access {
								t.Fatalf("native: gas=%+v access=%v; bytecode: gas=%+v access=%v", got.gas, got.access, want.gas, want.access)
							}
							if got.err == nil && !bytes.Equal(got.output, expected) {
								t.Fatalf("native read %x, want %x from %s", got.output, expected, target.address)
							}
							if got.err != nil && got.access != [4]bool{true, warm.selfSlot, warm.account, warm.slot} {
								t.Fatalf("failed call did not restore the access list: %v", got.access)
							}
						})
					}
				})
			}
		}
	}
}
