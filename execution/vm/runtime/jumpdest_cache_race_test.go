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

package runtime

import (
	"crypto/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// jumpdestCodes returns two codes with a random tail after STOP, which keeps the process-global
// JUMPDEST analysis cache entries to one test: oldCode jumps over PUSH2 data at bytes 4..5, and
// newCode jumps to a JUMPDEST at byte 4.
func jumpdestCodes() (oldCode, newCode accounts.Code) {
	salt := make([]byte, 16)
	_, _ = rand.Read(salt)
	oldCode = accounts.NewCode(append([]byte{
		byte(vm.PUSH1), 6, byte(vm.JUMP), byte(vm.PUSH2), 0, 0, byte(vm.JUMPDEST), byte(vm.STOP),
	}, salt...))
	newCode = accounts.NewCode(append([]byte{
		byte(vm.PUSH1), 4, byte(vm.JUMP), byte(vm.STOP), byte(vm.JUMPDEST), byte(vm.STOP),
	}, salt...))
	return oldCode, newCode
}

func deployVersioned(vmap *state.VersionMap, addr accounts.Address, code accounts.Code, v state.Version) {
	acc := accounts.NewAccount()
	acc.CodeHash = code.Hash
	vmap.WriteAddress(addr, v, &acc, true)
	vmap.WriteCode(addr, v, code, true)
	vmap.WriteCodeHash(addr, v, code.Hash, true)
}

// callWhilePublishing calls addr as the transaction with index 2 and publishes the code written by
// the transaction with index 1 between the reads of the callee's code and of its code hash.
func callWhilePublishing(t *testing.T, cfg *chain.Config, vmap *state.VersionMap, addr accounts.Address, publish func()) {
	t.Helper()
	ibs := state.NewWithVersionMap(state.NewNoopReader(), vmap)
	ibs.SetNoMaterialize(true)
	defer ibs.Close()
	ibs.SetTxContext(1, 2)
	onEnter := func(depth int, _ byte, _, _ accounts.Address, _ bool, _ []byte, _ uint64, _ uint256.Int, _ []byte) {
		if depth == 0 {
			publish()
		}
	}
	_, _, err := Call(addr, nil, &Config{
		ChainConfig: cfg,
		GasLimit:    100_000,
		State:       ibs,
		EVMConfig:   vm.Config{Tracer: &tracing.Hooks{OnEnter: onEnter}},
	})
	require.NoError(t, err)
}

// callCode executes code at addr in a fresh state.
func callCode(t *testing.T, cfg *chain.Config, addr accounts.Address, code []byte) error {
	t.Helper()
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeUnspecified))
	_, _, err := Call(addr, nil, &Config{ChainConfig: cfg, GasLimit: 100_000, State: ibs})
	return err
}

// TestJumpDestCacheCodeRedeployed covers a speculative execution of a call to a contract that a
// prior transaction of the same block redeploys with different code (SELFDESTRUCT and CREATE2
// before Cancun): the JUMPDEST analysis of the old code must not be cached for the new code.
func TestJumpDestCacheCodeRedeployed(t *testing.T) {
	t.Parallel()
	cfg := chain.TestChainBerlinConfig
	oldCode, newCode := jumpdestCodes()
	addr := accounts.InternAddress(common.HexToAddress("0xc0de"))

	vmap := state.NewVersionMap(nil)
	deployVersioned(vmap, addr, oldCode, state.Version{TxIndex: 0})
	callWhilePublishing(t, cfg, vmap, addr, func() {
		vmap.WriteCode(addr, state.Version{TxIndex: 1}, newCode, true)
		vmap.WriteCodeHash(addr, state.Version{TxIndex: 1}, newCode.Hash, true)
	})

	require.NoError(t, callCode(t, cfg, addr, newCode.Bytes),
		"the new code must not use the JUMPDEST analysis of the old code")
}

// TestJumpDestCacheDelegateRedeployed is TestJumpDestCacheCodeRedeployed for a call through an
// EIP-7702 authority whose delegate is redeployed.
func TestJumpDestCacheDelegateRedeployed(t *testing.T) {
	t.Parallel()
	cfg := chain.TestChainOsakaConfig
	oldCode, newCode := jumpdestCodes()
	authority := accounts.InternAddress(common.HexToAddress("0xa0"))
	delegate := accounts.InternAddress(common.HexToAddress("0xd0"))

	vmap := state.NewVersionMap(nil)
	deployVersioned(vmap, delegate, oldCode, state.Version{TxIndex: 0})
	deployVersioned(vmap, authority, accounts.NewCode(types.AddressToDelegation(delegate)), state.Version{TxIndex: 0})
	callWhilePublishing(t, cfg, vmap, authority, func() {
		vmap.WriteCode(delegate, state.Version{TxIndex: 1}, newCode, true)
		vmap.WriteCodeHash(delegate, state.Version{TxIndex: 1}, newCode.Hash, true)
	})

	require.NoError(t, callCode(t, cfg, delegate, newCode.Bytes),
		"the redeployed delegate's code must not use the JUMPDEST analysis of its old code")
}
