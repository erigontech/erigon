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

package chain

import (
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// PrecompiledContract is the basic interface for native Go contracts. The implementation
// requires a deterministic gas count based on the input size of the Run method of the
// contract.
//
// It lives in the chain package (rather than execution/vm) so that a chain's
// precompile set can be carried on Config/Rules without an import cycle; vm
// re-exports it as a type alias.
type PrecompiledContract interface {
	RequiredGas(input []byte) uint64  // RequiredPrice calculates the contract gas use
	Run(input []byte) ([]byte, error) // Run runs the precompiled contract
	Name() string
}

// PrecompiledContracts contains the precompiled contracts supported at a given fork.
type PrecompiledContracts map[accounts.Address]PrecompiledContract

// PrecompileForkId identifies the precompile set active at a fork. The order
// must match the fork ladder; ForkId derives the value from Rules so the two
// stay in lockstep.
type PrecompileForkId uint8

const (
	PrecompilesHomestead PrecompileForkId = iota
	PrecompilesByzantium
	PrecompilesIstanbul
	PrecompilesBerlin
	PrecompilesCancun
	PrecompilesNapoli
	PrecompilesPrague
	PrecompilesBhilai
	PrecompilesOsaka
	numPrecompileForks
)

// PrecompileForkId returns the precompile set active under these rules.
func (r *Rules) PrecompileForkId() PrecompileForkId {
	switch {
	case r.IsOsaka:
		return PrecompilesOsaka
	case r.IsBhilai:
		return PrecompilesBhilai
	case r.IsPrague:
		return PrecompilesPrague
	case r.IsNapoli:
		return PrecompilesNapoli
	case r.IsCancun:
		return PrecompilesCancun
	case r.IsBerlin:
		return PrecompilesBerlin
	case r.IsIstanbul:
		return PrecompilesIstanbul
	case r.IsByzantium:
		return PrecompilesByzantium
	default:
		return PrecompilesHomestead
	}
}

// ChainPrecompiles is a chain's precompile set, resolved per fork once at chain
// initialization. It is immutable after construction, so each chain owns an
// isolated set with no shared mutable state — there is nothing to lock and
// nothing to leak between chains in a multi-chain process.
//
// Construct it via vm.NewChainPrecompiles (which supplies the base maps) and
// assign to Config.Precompiles before execution starts. A nil set means the
// built-in default precompiles are used (the standard single-chain path).
type ChainPrecompiles struct {
	byFork [numPrecompileForks]PrecompiledContracts
	addrs  [numPrecompileForks][]accounts.Address
}

// NewChainPrecompiles builds an immutable per-chain set from the fully-resolved
// (base merged with any custom) precompile map for each fork. The active-address
// list for each fork is precomputed so per-transaction access-list warming does
// not allocate.
func NewChainPrecompiles(byFork map[PrecompileForkId]PrecompiledContracts) *ChainPrecompiles {
	cp := &ChainPrecompiles{}
	for fork, m := range byFork {
		cp.byFork[fork] = m
		addrs := make([]accounts.Address, 0, len(m))
		for a := range m {
			addrs = append(addrs, a)
		}
		cp.addrs[fork] = addrs
	}
	return cp
}

// Contracts returns the precompiled contracts active under the given rules.
func (cp *ChainPrecompiles) Contracts(r *Rules) PrecompiledContracts {
	return cp.byFork[r.PrecompileForkId()]
}

// Addresses returns the active precompile addresses under the given rules. The
// returned slice is shared and must not be mutated.
func (cp *ChainPrecompiles) Addresses(r *Rules) []accounts.Address {
	return cp.addrs[r.PrecompileForkId()]
}

// Config.Precompiles is json:"-" — it is attached at chain initialization and lost
// whenever the ChainConfig is re-materialised from the DB (ReadChainConfig). That is
// fine for the execution path (which re-attaches at startup), but the RPC layer
// re-reads the config independently for receipt/eth_getLogs re-execution, and so its
// EVM would silently lack any custom precompiles — a tx that calls one then
// regenerates to a DIFFERENT receipt than consensus produced.
//
// The registry below lets a node register its runtime precompile set by chain id at
// startup so those re-materialised configs can re-attach it (see
// ChainConfig.attachRegisteredPrecompiles). Register once, before serving RPC.
var (
	registeredPrecompilesMu sync.RWMutex
	registeredPrecompiles   = map[string]*ChainPrecompiles{}
)

// RegisterPrecompiles records a chain's runtime precompile set by chain id.
func RegisterPrecompiles(chainID *uint256.Int, cp *ChainPrecompiles) {
	if chainID == nil || cp == nil {
		return
	}
	registeredPrecompilesMu.Lock()
	registeredPrecompiles[chainID.Hex()] = cp
	registeredPrecompilesMu.Unlock()
}

// RegisteredPrecompiles returns the precompile set registered for chainID, or nil.
func RegisteredPrecompiles(chainID *uint256.Int) *ChainPrecompiles {
	if chainID == nil {
		return nil
	}
	registeredPrecompilesMu.RLock()
	defer registeredPrecompilesMu.RUnlock()
	return registeredPrecompiles[chainID.Hex()]
}
