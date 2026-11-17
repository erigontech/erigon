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
	"errors"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/vm"
)

// cas20MarkerPrefix opens every CAS20 token address.
var cas20MarkerPrefix = [2]byte{0xca, 0x52}

var (
	CAS20FactoryAddress            = common.HexToAddress("0xCA5F000000000000000000000000000000000000")
	CAS20ActivationRegistryAddress = common.HexToAddress("0x7020000000000000000000000000000000000001")
	CAS20PolicyRegistryAddress     = common.HexToAddress("0x7020000000000000000000000000000000000002")
	// CAS20GovHubAddress is BSC's GovHub, the only account the activation registry
	// takes parameter changes from.
	CAS20GovHubAddress = common.HexToAddress("0x0000000000000000000000000000000000001007")
)

// CAS20MarkerCode is the code of every initialized CAS20 account.
var CAS20MarkerCode = []byte{0xEF}

var cas20MarkerCodeHash = keccakHash(CAS20MarkerCode)

var (
	ErrCAS20DelegateCall      = errors.New("cas20: delegate call not allowed")
	ErrCAS20StatelessDispatch = errors.New("cas20: stateful precompile invoked without state")
)

// IsCAS20Address: byte[0:2] == 0xCA52 and byte[2:10] all zero.
func IsCAS20Address(addr common.Address) bool {
	if addr[0] != cas20MarkerPrefix[0] || addr[1] != cas20MarkerPrefix[1] {
		return false
	}
	for i := 2; i < 10; i++ {
		if addr[i] != 0 {
			return false
		}
	}
	return true
}

// Exact-hash, not non-empty: foreign code is not a token.
func cas20InitializedMetered(ctx *PrecompileContext, addr common.Address) bool {
	if !ctx.chargeAccountAccess(addr) {
		return false
	}
	return ctx.StateDB.GetCodeHash(addr) == cas20MarkerCodeHash
}

// Any code counts, not just the sentinel: overwriting foreign code would destroy it.
func cas20AddressOccupied(ctx *PrecompileContext, addr common.Address) bool {
	if !ctx.chargeAccountAccess(addr) {
		// True is the safe answer when the frame can no longer pay to find out.
		return true
	}
	return !hadNoCode(ctx.StateDB, addr)
}

// No code at all is the condition under which writing code owes the
// account-creation cost.
func hadNoCode(state StateDB, addr common.Address) bool {
	ch := state.GetCodeHash(addr)
	return ch == (common.Hash{}) || ch == empty.CodeHash
}

// cas20EnterCall is the prologue every CAS20 entry point runs.
func cas20EnterCall(ctx *PrecompileContext, input []byte) error {
	if !ctx.DirectCall {
		return ErrCAS20DelegateCall
	}
	if ctx.Value != nil && !ctx.Value.IsZero() {
		return revCAS20("NonPayable()", errSelNonPayable)
	}
	if !ctx.chargeCalldata(input) {
		return vm.ErrOutOfGas
	}
	return nil
}

func keccakHash(data ...[]byte) common.Hash { return common.BytesToHash(crypto.Keccak256(data...)) }

func hashBytes(h common.Hash) []byte { return h[:] }

func addrBytes(a common.Address) []byte { return a[:] }
