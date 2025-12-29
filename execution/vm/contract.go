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

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// globalJumpDestCache is a global thread-safe LRU cache for JUMPDEST analysis.
// Since contract code is immutable once deployed, the analysis can be cached permanently.
// This cache persists across blocks and transactions.
var globalJumpDestCache, _ = lru.New[accounts.CodeHash, bitvec](10_000)

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
	caller    accounts.Address
	addr      accounts.Address
	jumpdests *JumpDestCache // Aggregated result of JUMPDEST analysis.
	analysis  bitvec         // Locally cached result of JUMPDEST analysis

	Code     []byte
	CodeHash accounts.CodeHash

	value uint256.Int
}

type JumpDestCache struct {
	*simplelru.LRU[accounts.CodeHash, bitvec]
	hit, total int
	trace      bool
}

var (
	JumpDestCacheLimit = dbg.EnvInt("JD_LRU", 128)
	jumpDestCacheTrace = dbg.EnvBool("JD_LRU_TRACE", false)
)

func NewJumpDestCache(limit int) *JumpDestCache {
	c, err := simplelru.NewLRU[accounts.CodeHash, bitvec](limit, nil)
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
func NewContract(caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, value uint256.Int, jumpDest *JumpDestCache) *Contract {
	return &Contract{
		caller:    callerAddress,
		addr:      addr,
		value:     value,
		jumpdests: jumpDest,
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
	// Fast path: if we already have the analysis cached locally, use it directly
	if c.analysis != nil {
		return c.analysis.codeSegment(udest)
	}

	// Do we have a contract hash already?
	// If we do have a hash, that means it's a 'regular' contract. For regular
	// contracts (not temporary initcode), we can use the global cache.
	if !c.CodeHash.IsZero() {
		// Check global cache first (thread-safe, persists across blocks)
		if analysis, ok := globalJumpDestCache.Get(c.CodeHash); ok {
			c.analysis = analysis
			return c.analysis.codeSegment(udest)
		}

		// Not in global cache, compute and store
		analysis := codeBitmap(c.Code)
		globalJumpDestCache.Add(c.CodeHash, analysis)
		c.analysis = analysis
		return c.analysis.codeSegment(udest)
	}

	// We don't have the code hash, most likely a piece of initcode not already
	// in state trie. In that case, we do an analysis, and save it locally, so
	// we don't have to recalculate it for every JUMP instruction in the execution
	// However, we don't save it within the global cache (initcode is temporary)
	c.analysis = codeBitmap(c.Code)
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
