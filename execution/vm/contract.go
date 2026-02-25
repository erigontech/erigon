// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package vm

import (
	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// AccountRef is a reference to an account address.
//
// Account references are used during EVM initialisation and
// it's primary use is to fetch addresses. Removing this object
// proves difficult because of the cached jump destinations which
// are fetched from the parent contract (i.e. the caller).
type AccountRef accounts.Address

// Address casts AccountRef to a Address
func (ar AccountRef) Address() accounts.Address { return (accounts.Address)(ar) }

// Contract represents an ethereum contract in the state database. It contains
// the contract code, calling arguments. Contract implements ContractRef
type Contract struct {
	// caller is the result of the caller which initialised this
	// contract. However when the "call method" is delegated this value
	// needs to be initialised to that of the caller's caller.
	caller   accounts.Address
	addr     accounts.Address
	analysis bitvec // Locally cached result of JUMPDEST analysis

	Code     []byte
	CodeHash accounts.CodeHash

	value uint256.Int

	Gas              uint64
	UsedMultiGas     multigas.MultiGas
	RetainedMultiGas multigas.MultiGas

	DelegateOrCallcode bool
	IsDeployment       bool
	IsSystemCall       bool
}

// around 64MB cache in the worst case.
var jumpDestCache = cache.NewGenericCache[bitvec](64*datasize.MB, func(v bitvec) int { return len(v) })

// NewContract returns a new contract environment for the execution of EVM.
func NewContract(caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, value uint256.Int) *Contract {
	return &Contract{
		caller: callerAddress,
		addr:   addr,
		value:  value,
	}
}

// First result tells us if the destination is valid
// Second result tells us if the code bitmap was used
func (c *Contract) validJumpdest(dest uint256.Int) (bool, bool) {
	udest, overflow := dest.Uint64WithOverflow()
	// PC cannot go beyond len(code) and certainly can't be bigger than 64bits.
	// Don't bother checking for JUMPDEST in that case.
	if overflow || udest >= uint64(len(c.Code)) {
		return false, false
	}
	// Only JUMPDESTs allowed for destinations
	if OpCode(c.Code[udest]) != JUMPDEST {
		return false, false
	}
	return c.isCode(udest), true
}

// isCode returns true if the provided PC location is an actual opcode, as
// opposed to a data-segment following a PUSHN operation.
func (c *Contract) isCode(udest uint64) bool {
	if c.analysis != nil {
		return c.analysis.codeSegment(udest)
	}
	var codeHash common.Hash
	isCodeHashZero := c.CodeHash.IsZero()
	if !isCodeHashZero {
		codeHash = c.CodeHash.Value()
	}

	if !isCodeHashZero {
		if analysis, ok := jumpDestCache.Get(codeHash[:]); ok {
			c.analysis = analysis
			return c.analysis.codeSegment(udest)
		}
	}

	c.analysis = codeBitmap(c.Code)

	if !isCodeHashZero {
		jumpDestCache.Put(codeHash[:], c.analysis)
	}

	return c.analysis.codeSegment(udest)
}

// GetOp returns the n'th element in the contract's byte array
func (c *Contract) GetOp(n uint64) OpCode {
	if n < uint64(len(c.Code)) {
		return OpCode(c.Code[n])
	}

	return STOP
}

// Caller returns the caller of the contract.
//
// Caller will recursively set to the caller's caller when
// the contract is a delegate call, including that of caller's caller.
func (c *Contract) Caller() accounts.Address {
	return c.caller
}

// Address returns the contracts address
func (c *Contract) Address() accounts.Address {
	return c.addr
}

// Value returns the contract's value (sent to it from it's caller)
func (c *Contract) Value() uint256.Int {
	return c.value
}

func (c *Contract) IsDelegateOrCallcode() bool {
	return c.DelegateOrCallcode
}

func (c *Contract) UseGas(cost uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) bool {
	if cost > c.Gas {
		return false
	}
	c.Gas -= cost
	return true
}

func (c *Contract) UseMultiGas(mg multigas.MultiGas, tracer *tracing.Hooks, reason tracing.GasChangeReason) bool {
	cost := mg.SingleGas()
	if cost > c.Gas {
		return false
	}
	c.Gas -= cost
	c.UsedMultiGas.SaturatingAddInto(mg)
	return true
}

func (c *Contract) GetTotalUsedMultiGas() multigas.MultiGas {
	return c.UsedMultiGas.SaturatingSub(c.RetainedMultiGas)
}
