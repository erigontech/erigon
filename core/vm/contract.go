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
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/core/tracing"
)

// ContractRef is a reference to the contract's backing object
type ContractRef interface {
	Address() common.Address
}

// AccountRef implements ContractRef.
//
// Account references are used during EVM initialisation and
// it's primary use is to fetch addresses. Removing this object
// proves difficult because of the cached jump destinations which
// are fetched from the parent contract (i.e. the caller), which
// is a ContractRef.
type AccountRef common.Address

// Address casts AccountRef to a Address
func (ar AccountRef) Address() common.Address { return (common.Address)(ar) }

// Contract represents an ethereum contract in the state database. It contains
// the contract code, calling arguments. Contract implements ContractRef
type Contract struct {
	// CallerAddress is the result of the caller which initialised this
	// contract. However when the "call method" is delegated this value
	// needs to be initialised to that of the caller's caller.
	CallerAddress common.Address
	caller        ContractRef
	self          common.Address
	jumpdests     *JumpDestCache // Aggregated result of JUMPDEST analysis.
	analysis      bitvec         // Locally cached result of JUMPDEST analysis
	skipAnalysis  bool

	Code     []byte
	CodeHash common.Hash
	CodeAddr *common.Address
	Input    []byte

	Gas   uint64
	value *uint256.Int
}

type JumpDestCache struct {
	*simplelru.LRU[common.Hash, bitvec]
	hit, total int
	trace      bool
}

var (
	JumpDestCacheLimit = dbg.EnvInt("JD_LRU", 128)
	jumpDestCacheTrace = dbg.EnvBool("JD_LRU_TRACE", false)
)

func NewJumpDestCache(limit int) *JumpDestCache {
	c, err := simplelru.NewLRU[common.Hash, bitvec](limit, nil)
	if err != nil {
		panic(err)
	}
	return &JumpDestCache{LRU: c, trace: jumpDestCacheTrace}
}

func (c *JumpDestCache) LogStats() {
	if c == nil || !c.trace {
		return
	}
	log.Warn("[dbg] JumpDestCache", "hit", c.hit, "total", c.total, "limit", JumpDestCacheLimit, "ratio", fmt.Sprintf("%.2f", float64(c.hit)/float64(c.total)))
}

// NewContract returns a new contract environment for the execution of EVM.
func NewContract(caller ContractRef, addr common.Address, value *uint256.Int, gas uint64, skipAnalysis bool, jumpDest *JumpDestCache) *Contract {
	return &Contract{
		CallerAddress: caller.Address(), caller: caller, self: addr,
		value:        value,
		skipAnalysis: skipAnalysis,
		// Gas should be a pointer so it can safely be reduced through the run
		// This pointer will be off the state transition
		Gas:       gas,
		jumpdests: jumpDest,
	}
}

// First result tells us if the destination is valid
// Second result tells us if the code bitmap was used
func (c *Contract) validJumpdest(dest *uint256.Int) (bool, bool) {
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
	if c.skipAnalysis {
		return true, false
	}
	return c.isCode(udest), true
}

// isCode returns true if the provided PC location is an actual opcode, as
// opposed to a data-segment following a PUSHN operation.
func (c *Contract) isCode(udest uint64) bool {
	// Do we have a contract hash already?
	// If we do have a hash, that means it's a 'regular' contract. For regular
	// contracts ( not temporary initcode), we store the analysis in a map
	if c.CodeHash != (common.Hash{}) {
		// Does parent context have the analysis?
		c.jumpdests.total++
		analysis, exist := c.jumpdests.Get(c.CodeHash)
		if !exist {
			// Do the analysis and save in parent context
			// We do not need to store it in c.analysis
			analysis = codeBitmap(c.Code)
			c.jumpdests.Add(c.CodeHash, analysis)
		} else {
			c.jumpdests.hit++
		}
		// Also stash it in current contract for faster access
		c.analysis = analysis
		return c.analysis.codeSegment(udest)
	}

	// We don't have the code hash, most likely a piece of initcode not already
	// in state trie. In that case, we do an analysis, and save it locally, so
	// we don't have to recalculate it for every JUMP instruction in the execution
	// However, we don't save it within the parent context
	if c.analysis == nil {
		c.analysis = codeBitmap(c.Code)
	}

	return c.analysis.codeSegment(udest)
}

// AsDelegate sets the contract to be a delegate call and returns the current
// contract (for chaining calls)
func (c *Contract) AsDelegate() *Contract {
	// NOTE: caller must, at all times be a contract. It should never happen
	// that caller is something other than a Contract.
	parent := c.caller.(*Contract)
	c.CallerAddress = parent.CallerAddress
	c.value = parent.value

	return c
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
// Caller will recursively call caller when the contract is a delegate
// call, including that of caller's caller.
func (c *Contract) Caller() common.Address {
	return c.CallerAddress
}

// UseGas attempts the use gas and subtracts it and returns true on success
func (c *Contract) UseGas(gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) (ok bool) {
	// We collect the gas change reason today, future changes will add gas change(s) tracking with reason
	_ = reason

	if c.Gas < gas {
		return false
	}

	if tracer != nil && tracer.OnGasChange != nil && reason != tracing.GasChangeIgnored {
		tracer.OnGasChange(c.Gas, c.Gas-gas, reason)
	}
	c.Gas -= gas
	return true
}

// RefundGas refunds gas to the contract
func (c *Contract) RefundGas(gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) {
	// We collect the gas change reason today, future changes will add gas change(s) tracking with reason
	_ = reason

	if gas == 0 {
		return
	}
	if tracer != nil && tracer.OnGasChange != nil && reason != tracing.GasChangeIgnored {
		tracer.OnGasChange(c.Gas, c.Gas+gas, reason)
	}
	c.Gas += gas
}

// Address returns the contracts address
func (c *Contract) Address() common.Address {
	return c.self
}

// Value returns the contract's value (sent to it from it's caller)
func (c *Contract) Value() *uint256.Int {
	return c.value
}

// SetCallCode sets the code of the contract and address of the backing data
// object
func (c *Contract) SetCallCode(addr *common.Address, hash common.Hash, code []byte) {
	c.Code = code
	c.CodeHash = hash
	c.CodeAddr = addr
}

// SetCodeOptionalHash can be used to provide code, but it's optional to provide hash.
// In case hash is not provided, the jumpdest analysis will not be saved to the parent context
func (c *Contract) SetCodeOptionalHash(addr *common.Address, codeAndHash *codeAndHash) {
	c.Code = codeAndHash.code
	c.CodeHash = codeAndHash.hash
	c.CodeAddr = addr
}
