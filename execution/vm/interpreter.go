// Copyright 2014 The go-ethereum Authors
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
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Config are the configuration options for the Interpreter
type Config struct {
	Tracer        *tracing.Hooks
	NoRecursion   bool // Disables call, callcode, delegate call and create
	NoBaseFee     bool // Forces the EIP-1559 baseFee to 0 (needed for 0 price calls)
	TraceJumpDest bool // Print transaction hashes where jumpdest analysis was useful
	NoReceipts    bool // Do not calculate receipts
	ReadOnly      bool // Do no perform any block finalisation
	StatelessExec bool // true is certain conditions (like state trie root hash matching) need to be relaxed for stateless EVM execution
	RestoreState  bool // Revert all changes made to the state (useful for constant system calls)

	ExtraEips []int // Additional EIPS that are to be enabled
}

func (vmConfig *Config) HasEip3860(rules *chain.Rules) bool {
	return slices.Contains(vmConfig.ExtraEips, 3860) || rules.IsShanghai
}

// CallContext contains the things that are per-call, such as stack and memory,
// but not transients like pc and gas
type CallContext struct {
	gas      uint64
	stateGas uint64
	input    []byte
	Memory   Memory

	// Two-tier intern cache for the top-of-stack key/address bytes:
	//
	//   Tier 1 (validity flag): same-opcode dispatch reuses the prior
	//     interned value via cachedKeyValid. The interpreter clears the
	//     flag on every opcode dispatch; the flag is set when the cache
	//     is populated. Default-zero (false) is correctly "invalid", so
	//     struct-literal CallContexts (e.g. unit tests) miss safely.
	//
	//   Tier 2 (direct-mapped associative cache): the first uint64 of the
	//     stack-top bytes selects a slot in cachedKeyTable; the slot holds
	//     the FULL bytes32 plus the interned handle. On lookup we mask the
	//     uint64 to size, compare the full bytes against the slot, return
	//     the cached handle on equal; otherwise unique.Make and replace the
	//     slot. The full-equal check prevents false positives from
	//     first-uint64 collisions. On `sload_same_key` (pprof: 22 % of TEST-
	//     block samples on accounts.InternKey via peekStorageKey +
	//     gasSLoadEIP2929) the table catches every successive same-key SLOAD
	//     in O(1) without unique.Make. Per-CallContext cost: 16 × 40 = 640
	//     bytes for keys + 16 × 28 = 448 bytes for addresses; pool-amortised.
	//
	// Placed before Stack so these fields stay in L1D rather than being pushed
	// out by Stack.data (32 KB).
	cachedKeyValid  bool
	cachedAddrValid bool
	cachedKey       accounts.StorageKey
	cachedAddr      accounts.Address
	cachedKeyTable  [internTableSize]internStorageKeyEntry
	cachedAddrTable [internTableSize]internAddressEntry

	// Frame-local slot cache (per project_frame_local_slot_cache_with_rollup.md).
	// Holds *WriteCell[uint256.Int] pointers so the cache shape matches the
	// cell-pipeline endpoint: same WriteCell type that lives in versionMap
	// and (eventually) SharedDomains.  SLOAD reaccess within the same frame
	// reads cell.Value and skips the versionedReadCore + GetState chain.
	// SSTORE replaces the cache entry with the new cell so the next SLOAD
	// sees the post-write value without re-probing.
	//
	// Lifetime: per-CallContext.  Lookup walks up the parent chain via
	// CallContext.parent.  On normal return, the child's cache merges into
	// the parent's (rollup, pointer assignment).  On revert (error return),
	// the child's cache is discarded.  Both happen in Run's deferred
	// finaliser before put().
	//
	// Layout: two-level map keyed by Address (outer) and StorageKey (inner).
	// Both Address and StorageKey are unique.Handle = single 8-byte pointer,
	// so each level qualifies for Go's runtime mapaccess_fast64 fast path
	// (vs the generic mapaccess used for the prior 16-byte struct key).
	// On sload-same-key, 2 fast64 probes beat 1 generic probe by enough to
	// offset the second map lookup.
	slotCache map[accounts.Address]map[accounts.StorageKey]*state.WriteCell[uint256.Int]

	// parent is the CallContext that invoked this frame.  Used by SLOAD to
	// walk up the chain when the local cache misses, and by Run's deferred
	// finaliser to roll the child's cache up into the parent on success.
	// Nil at the outermost frame.
	parent *CallContext

	// cachedSlotCell hands a *WriteCell looked up by gasSLoadEIP2929 forward
	// to opSload so the op handler skips a second findSlotCell map probe on
	// the SLOAD cache-hit path.  Lifetime is a single opcode dispatch: the
	// interpreter Run loop clears it at the top of each iteration alongside
	// cachedKeyValid/cachedAddrValid.  Nil when no cell was prefetched (op
	// not SLOAD, or SLOAD missed in the cache and gas function fell through
	// to AddSlotToAccessList).
	cachedSlotCell *state.WriteCell[uint256.Int]

	// codeCache holds the per-address result of IBS.GetCode at first
	// access, so subsequent EXTCODESIZE / EXTCODEHASH / EXTCODECOPY for the
	// same address skip the versionedReadCore + GetCode chain in IBS.
	// Stores what IBS.GetCode returns (NOT what ResolveCode returns) — i.e.,
	// the EIP-7702 delegation marker bytes for a delegated EOA, raw code
	// otherwise.  That matches what EXTCODE* opcodes need; CALL machinery
	// uses ResolveCode and is intentionally not served by this cache.
	//
	// Key is accounts.Address = unique.Handle = 8-byte pointer ⇒ fast64
	// map probe.  Cache is per-frame; lookup walks the parent chain via
	// findCodeEntry.  Invalidation: opCreate / opCreate2 success calls
	// invalidateCodeEntry to delete the new address from the chain (the
	// only mid-EVM mutation of stored code; SELFDESTRUCT only marks
	// selfdestructed and FinalizeTx does the actual code deletion).
	codeCache map[accounts.Address]*codeCacheEntry

	// cachedCodeEntry hands a *codeCacheEntry looked up by an EXTCODE*
	// gas function forward to the op handler, mirroring cachedSlotCell.
	// Same lifetime: cleared at the top of each interpreter Run iteration.
	cachedCodeEntry *codeCacheEntry

	// codeHashCache is a hash-only sibling of codeCache for the EXTCODEHASH
	// path.  Value-typed map (CodeHash is a 32-byte fixed array) so a hit
	// returns the hash by value without an entry indirection.  Populated by
	// loadAndCacheCodeHash on EXTCODEHASH-miss (which avoids the bytecode
	// load that loadAndCacheCode performs) AND by loadAndCacheCode itself
	// when EXTCODESIZE/COPY warms a cell (so an EXTCODEHASH later in the
	// same frame is a hit either way).
	codeHashCache map[accounts.Address]accounts.CodeHash

	// cachedCodeHash + cachedCodeHashValid hand a CodeHash looked up by
	// gasExtCodeHashEIP2929 forward to opExtCodeHash.  Value+bool rather
	// than a pointer avoids the heap alloc per populate that a *CodeHash
	// handoff would force.
	cachedCodeHash      accounts.CodeHash
	cachedCodeHashValid bool

	// nonceCache holds per-address nonce reads to short-circuit
	// IBS.GetNonce on the CREATE / CREATE2 paths (the only EVM-internal
	// nonce read sites — there is no opcode that reads nonce directly).
	// SetNonce sites do a write-through so subsequent reads in the same
	// frame chain see the new value without an IBS round-trip.  Cache is
	// per-frame; lookup walks the parent chain via findNonce.  No
	// gas-fn→op handoff field — all consumers live in evm.go and read
	// the cache inline rather than through a CallContext field.
	nonceCache map[accounts.Address]uint64

	Stack    Stack
	Contract Contract
}

// codeCacheEntry is the per-address payload of the frame-local code cache.
// Code is what IBS.GetCode returns (raw code for normal contracts, the
// EIP-7702 delegation marker for delegated EOAs, nil/empty for missing or
// empty accounts).  Hash mirrors IBS.GetCodeHash for the same address (so
// EXTCODEHASH can return without a second IBS call).  Size is derivable
// from len(Code) and not stored separately.
type codeCacheEntry struct {
	Code []byte
	Hash accounts.CodeHash
}

// Frame-local code cache hit/miss counters.  Diagnostic only — incremented
// inside findCodeEntry / loadAndCacheCode; surfaced via DumpCodeCacheStats
// at process / bench end so we can verify the cache is actually catching
// repeated EXT* reads as expected for a given workload.
var (
	codeCacheHitCount  atomic.Uint64
	codeCacheMissCount atomic.Uint64
)

// DumpCodeCacheStats returns a snapshot of frame-local code-cache hit/miss
// counters.  Callers (typically bench harness / engine_x runner) use this
// to confirm the cache is being exercised as expected on a given workload.
func DumpCodeCacheStats() (hits, misses uint64) {
	return codeCacheHitCount.Load(), codeCacheMissCount.Load()
}

// Frame-local codehash cache hit/miss counters.  Same purpose as the code
// cache counters but for the hash-only sibling cache consulted by
// gasExtCodeHashEIP2929 / opExtCodeHash.
var (
	codeHashCacheHitCount  atomic.Uint64
	codeHashCacheMissCount atomic.Uint64
)

// DumpCodeHashCacheStats snapshots the frame-local codehash cache counters.
func DumpCodeHashCacheStats() (hits, misses uint64) {
	return codeHashCacheHitCount.Load(), codeHashCacheMissCount.Load()
}

// Frame-local nonce cache hit/miss counters.  Same purpose as the other
// per-field cache counters but for the nonce cache consulted by evm.create
// / evm.Create on the CREATE/CREATE2 paths.
var (
	nonceCacheHitCount  atomic.Uint64
	nonceCacheMissCount atomic.Uint64
)

// DumpNonceCacheStats snapshots the frame-local nonce cache counters.
func DumpNonceCacheStats() (hits, misses uint64) {
	return nonceCacheHitCount.Load(), nonceCacheMissCount.Load()
}

// findSlotCell walks the parent chain from this frame upward looking for
// a cached *WriteCell for (addr, key).  Each level of the two-level map
// uses an 8-byte unique.Handle key which qualifies for the runtime's
// mapaccess_fast64 fast path, so a hit costs 2 fast probes rather than
// 1 generic probe on a 16-byte struct key.  Used by opSload, opSstore,
// and gasSStore* / gasSLoadEIP2929 for cache-warm shortcuts.
func (ctx *CallContext) findSlotCell(addr accounts.Address, key accounts.StorageKey) (*state.WriteCell[uint256.Int], bool) {
	for c := ctx; c != nil; c = c.parent {
		if inner, ok := c.slotCache[addr]; ok {
			if cell, ok := inner[key]; ok {
				return cell, true
			}
		}
	}
	return nil, false
}

// findCodeEntry walks the parent chain looking for a cached codeCacheEntry
// for addr.  Used by EXTCODESIZE / EXTCODEHASH / EXTCODECOPY op handlers
// and their gas functions.  Each probe is a fast64 map lookup on the
// 8-byte unique.Handle Address key.
func (ctx *CallContext) findCodeEntry(addr accounts.Address) (*codeCacheEntry, bool) {
	for c := ctx; c != nil; c = c.parent {
		if entry, ok := c.codeCache[addr]; ok {
			codeCacheHitCount.Add(1)
			return entry, true
		}
	}
	codeCacheMissCount.Add(1)
	return nil, false
}

// invalidateCodeEntry removes any cached codeCacheEntry for addr from this
// frame and every ancestor.  Called by opCreate / opCreate2 success after
// SetCode lands a new contract at addr — the prior cached "no code" entry
// (if any) is now stale and a subsequent EXTCODE* must re-read from IBS.
// Cost is bounded by the frame depth (typically 1–2).
func (ctx *CallContext) invalidateCodeEntry(addr accounts.Address) {
	for c := ctx; c != nil; c = c.parent {
		delete(c.codeCache, addr)
	}
}

// findCodeHash walks the parent chain for a cached codehash entry.
// Mirrors findCodeEntry but for the hash-only sibling cache.
func (ctx *CallContext) findCodeHash(addr accounts.Address) (accounts.CodeHash, bool) {
	for c := ctx; c != nil; c = c.parent {
		if h, ok := c.codeHashCache[addr]; ok {
			codeHashCacheHitCount.Add(1)
			return h, true
		}
	}
	codeHashCacheMissCount.Add(1)
	return accounts.CodeHash{}, false
}

// invalidateCodeHash removes any cached codehash for addr from this frame
// and every ancestor.  Called alongside invalidateCodeEntry on CREATE /
// CREATE2 success.
func (ctx *CallContext) invalidateCodeHash(addr accounts.Address) {
	for c := ctx; c != nil; c = c.parent {
		delete(c.codeHashCache, addr)
	}
}

// findNonce walks the parent chain for a cached nonce entry.  Mirrors
// findCodeEntry but value-typed.
func (ctx *CallContext) findNonce(addr accounts.Address) (uint64, bool) {
	for c := ctx; c != nil; c = c.parent {
		if n, ok := c.nonceCache[addr]; ok {
			nonceCacheHitCount.Add(1)
			return n, true
		}
	}
	nonceCacheMissCount.Add(1)
	return 0, false
}

// cacheNonce records nonce as the current value for addr in this frame's
// cache.  Used on both miss-fill (after an IBS read) and write-through
// (after SetNonce).  The current-frame write shadows any stale ancestor
// entry via findNonce's parent-chain order; the rollup at frame return
// then propagates current-frame entries up to the parent on success,
// overwriting the stale value there too.
func (ctx *CallContext) cacheNonce(addr accounts.Address, nonce uint64) {
	if ctx.nonceCache == nil {
		ctx.nonceCache = map[accounts.Address]uint64{}
	}
	ctx.nonceCache[addr] = nonce
}

// internTableSize is the size of the direct-mapped intern caches on each
// CallContext. 16 slots covers the typical EVM call-frame working set of
// hot keys/addresses; collisions on the first-uint64 index resolve via
// replacement (correctness preserved by the full-bytes equal check).
const internTableSize = 16

type internStorageKeyEntry struct {
	bytes [32]byte
	key   accounts.StorageKey
	valid bool
}

type internAddressEntry struct {
	bytes [20]byte
	addr  accounts.Address
	valid bool
}

// peekStorageKey returns the top-of-stack value as an interned StorageKey.
// Two-tier cache (see CallContext struct comment for full design):
//  1. Same opcode dispatch (cachedKeyValid): return cachedKey.
//  2. Direct-mapped table: index by the first uint64 of the raw bytes,
//     full-bytes-equal verify the slot; hit returns cached, miss runs
//     unique.Make and replaces the slot.
//
// On `sload_same_key` the first SLOAD of a slot misses the table, runs
// unique.Make once, populates the slot. Every subsequent same-key SLOAD
// hits the slot in O(1) and skips unique.Make. pprof: ~22 % of TEST-block
// CPU on this workload was unique.Make via peekStorageKey + gas path.
func (ctx *CallContext) peekStorageKey() accounts.StorageKey {
	if ctx.cachedKeyValid {
		return ctx.cachedKey
	}
	bytes := ctx.Stack.peek().Bytes32()
	idx := binary.BigEndian.Uint64(bytes[:8]) & (internTableSize - 1)
	e := &ctx.cachedKeyTable[idx]
	if e.valid && e.bytes == bytes {
		ctx.cachedKey = e.key
		ctx.cachedKeyValid = true
		return e.key
	}
	k := accounts.InternKey(bytes)
	e.bytes = bytes
	e.key = k
	e.valid = true
	ctx.cachedKey = k
	ctx.cachedKeyValid = true
	return k
}

// peekAddress mirrors peekStorageKey for addresses (bytes20 instead of
// bytes32). The first 8 bytes of the address index the direct-mapped table.
func (ctx *CallContext) peekAddress() accounts.Address {
	if ctx.cachedAddrValid {
		return ctx.cachedAddr
	}
	bytes := ctx.Stack.peek().Bytes20()
	idx := binary.BigEndian.Uint64(bytes[:8]) & (internTableSize - 1)
	e := &ctx.cachedAddrTable[idx]
	if e.valid && e.bytes == bytes {
		ctx.cachedAddr = e.addr
		ctx.cachedAddrValid = true
		return e.addr
	}
	a := accounts.InternAddress(bytes)
	e.bytes = bytes
	e.addr = a
	e.valid = true
	ctx.cachedAddr = a
	ctx.cachedAddrValid = true
	return a
}

var contextPool = sync.Pool{
	New: func() any {
		return &CallContext{}
	},
}

func getCallContext(contract Contract, input []byte, gas mdgas.MdGas) *CallContext {
	ctx, ok := contextPool.Get().(*CallContext)
	if !ok {
		log.Error("Type assertion failure", "err", "cannot get Stack pointer from stackPool")
	}

	ctx.gas = gas.Regular
	ctx.stateGas = gas.State
	ctx.input = input
	ctx.Contract = contract
	return ctx
}

func (c *CallContext) put() {
	c.Memory.reset()
	c.Stack.Reset()
	c.cachedKeyValid = false
	c.cachedAddrValid = false
	// Zero the handles to release their canonMap pins while the context is
	// idle in the pool; unique.Handle values keep interned entries alive.
	c.cachedKey = accounts.NilKey
	c.cachedAddr = accounts.NilAddress
	// Clear the frame-local slot cache.  clear() preserves the map's
	// backing allocation for reuse on the next CallContext.Get from
	// the pool — cheaper than nil-and-realloc.
	if c.slotCache != nil {
		clear(c.slotCache)
	}
	if c.codeCache != nil {
		clear(c.codeCache)
	}
	if c.codeHashCache != nil {
		clear(c.codeHashCache)
	}
	if c.nonceCache != nil {
		clear(c.nonceCache)
	}
	c.cachedSlotCell = nil
	c.cachedCodeEntry = nil
	c.cachedCodeHash = accounts.CodeHash{}
	c.cachedCodeHashValid = false
	contextPool.Put(c)
}

// UseGas attempts the use gas and subtracts it and returns true on success
// We collect the gas change reason today, future changes will add gas change(s) tracking with reason
func (c *CallContext) useGas(gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) (ok bool) {
	if remaining, ok := useGas(c.gas, gas, tracer, reason); ok {
		c.gas = remaining
		return true
	}
	return false
}

func (c *CallContext) useMdGas(evm *EVM, gas uint64, t mdgas.MdGasType, tracer *tracing.Hooks, reason tracing.GasChangeReason) (ok bool) {
	remaining, ok := useMdGas(evm, c.Gas(), gas, t, tracer, reason)
	if ok {
		c.gas = remaining.Regular
		c.stateGas = remaining.State
		return true
	}
	return false
}

func useGas(initial uint64, gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) (remaining uint64, ok bool) {
	if initial < gas {
		return initial, false
	}

	if tracer != nil && tracer.OnGasChange != nil && reason != tracing.GasChangeIgnored {
		tracer.OnGasChange(initial, initial-gas, reason)
	}

	return initial - gas, true
}

func useMdGas(evm *EVM, initial mdgas.MdGas, gas uint64, t mdgas.MdGasType, tracer *tracing.Hooks, reason tracing.GasChangeReason) (mdgas.MdGas, bool) {
	var ok bool
	switch t {
	case mdgas.StateGas:
		originalGas := gas
		initial.State, ok = useGas(initial.State, gas, tracer, reason)
		if ok {
			if evm != nil {
				evm.stateGasConsumed += originalGas
			}
			return initial, true
		}
		// otherwise use up all remaining state gas and try to use some from the regular gas
		gas = gas - initial.State
		initial.State = 0
		initial.Regular, ok = useGas(initial.Regular, gas, tracer, reason)
		if ok && evm != nil {
			evm.stateGasConsumed += originalGas
		}
		return initial, ok
	case mdgas.RegularGas:
		initial.Regular, ok = useGas(initial.Regular, gas, tracer, reason)
		if ok && evm != nil {
			evm.regularGasConsumed += gas
		}
		return initial, ok
	default:
		panic(fmt.Errorf("useMdGas: invalid gas type: %d", t))
	}
}

// RefundGas refunds gas to the contract
func (c *CallContext) refundGas(gas uint64, tracer *tracing.Hooks, reason tracing.GasChangeReason) {
	// We collect the gas change reason today, future changes will add gas change(s) tracking with reason
	_ = reason

	if gas == 0 {
		return
	}
	if tracer != nil && tracer.OnGasChange != nil && reason != tracing.GasChangeIgnored {
		tracer.OnGasChange(c.gas, c.gas+gas, reason)
	}
	c.gas += gas
}

// MemoryData returns the underlying memory slice. Callers must not modify the contents
// of the returned data.
func (ctx *CallContext) MemoryData() []byte {
	return ctx.Memory.Data()
}

// StackData returns the stack data. Callers must not modify the contents
// of the returned data.
func (ctx *CallContext) StackData() []uint256.Int {
	return ctx.Stack.data[:ctx.Stack.top]
}

// Caller returns the current caller.
func (ctx *CallContext) Caller() accounts.Address {
	return ctx.Contract.Caller()
}

// Address returns the address where this scope of execution is taking place.
func (ctx *CallContext) Address() accounts.Address {
	return ctx.Contract.Address()
}

// CallValue returns the value supplied with this call.
func (ctx *CallContext) CallValue() uint256.Int {
	return ctx.Contract.Value()
}

// CallInput returns the input/calldata with this call. Callers must not modify
// the contents of the returned data.
func (ctx *CallContext) CallInput() []byte {
	return ctx.input
}

func (ctx *CallContext) Code() []byte {
	return ctx.Contract.Code
}

func (ctx *CallContext) CodeHash() accounts.CodeHash {
	return ctx.Contract.CodeHash
}

func (ctx *CallContext) Gas() mdgas.MdGas {
	return mdgas.MdGas{
		Regular: ctx.gas,
		State:   ctx.stateGas,
	}
}

// restoreChildGas returns the child frame's leftover gas to the parent.
// On success the parent adopts the child's remaining reservoir.
// On error handleFrameRevert adds childStateConsumed back to returnGas.State
// per EIP-8037: "all state gas consumed by the child… is restored to the
// parent's reservoir." Early-exit errors (collision, depth, insufficient
// balance) preserve gasRemaining.State so the reservoir is returned intact.
func (ctx *CallContext) restoreChildGas(returnGas mdgas.MdGas, tracer *tracing.Hooks) {
	ctx.stateGas = returnGas.State
	ctx.refundGas(returnGas.Regular, tracer, tracing.GasChangeCallLeftOverRefunded)
}

// callGas builds the MdGas to pass to a child CALL frame from the
// pre-computed callGasTemp (63/64 rule) and the current state reservoir.
func (ctx *CallContext) callGas(evm *EVM) mdgas.MdGas {
	return mdgas.MdGas{
		Regular: evm.CallGasTemp(),
		State:   ctx.stateGas,
	}
}

func copyJumpTable(jt *JumpTable) *JumpTable {
	var copy JumpTable
	for i, op := range jt {
		if op != nil {
			opCopy := *op
			copy[i] = &opCopy
		}
	}
	return &copy
}

func jumpTable(chainRules *chain.Rules, cfg Config) *JumpTable {
	var jt *JumpTable
	switch {
	case chainRules.IsAmsterdam:
		jt = &amsterdamInstructionSet
	case chainRules.IsOsaka:
		jt = &osakaInstructionSet
	case chainRules.IsBhilai:
		jt = &bhilaiInstructionSet
	case chainRules.IsPrague:
		jt = &pragueInstructionSet
	case chainRules.IsCancun:
		jt = &cancunInstructionSet
	case chainRules.IsNapoli:
		jt = &napoliInstructionSet
	case chainRules.IsShanghai:
		jt = &shanghaiInstructionSet
	case chainRules.IsLondon:
		jt = &londonInstructionSet
	case chainRules.IsBerlin:
		jt = &berlinInstructionSet
	case chainRules.IsIstanbul:
		jt = &istanbulInstructionSet
	case chainRules.IsConstantinople:
		jt = &constantinopleInstructionSet
	case chainRules.IsByzantium:
		jt = &byzantiumInstructionSet
	case chainRules.IsSpuriousDragon:
		jt = &spuriousDragonInstructionSet
	case chainRules.IsTangerineWhistle:
		jt = &tangerineWhistleInstructionSet
	case chainRules.IsHomestead:
		jt = &homesteadInstructionSet
	default:
		jt = &frontierInstructionSet
	}
	if len(cfg.ExtraEips) > 0 {
		jt = copyJumpTable(jt)
		for i, eip := range cfg.ExtraEips {
			if err := EnableEIP(eip, jt); err != nil {
				// Disable it, so caller can check if it's activated or not
				cfg.ExtraEips = append(cfg.ExtraEips[:i], cfg.ExtraEips[i+1:]...)
				log.Error("EIP activation failed", "eip", eip, "err", err)
			}
		}
	}

	return jt
}

// Run loops and evaluates the contract's code with the given input data and returns
// the return byte-slice and an error if one occurred.
//
// It's important to note that any errors returned by the interpreter should be
// considered a revert-and-consume-all-gas operation except for
// ErrExecutionReverted which means revert-and-keep-gas-left.
func (evm *EVM) Run(contract Contract, gas mdgas.MdGas, input []byte, readOnly bool) (_ []byte, _ mdgas.MdGas, err error) {
	// Don't bother with the execution if there's no code.
	if len(contract.Code) == 0 {
		return nil, gas, nil
	}

	// Reset the previous call's return data. It's unimportant to preserve the old buffer
	// as every returning call will return new data anyway.
	evm.returnData = nil

	var (
		op          OpCode // current opcode
		callContext = getCallContext(contract, input, gas)
		// For optimisation reason we're using uint64 as the program counter.
		// It's theoretically possible to go above 2^64. The YP defines the PC
		// to be uint256. Practically much less so feasible.
		pc   = uint64(0) // program counter
		cost uint64
		// copies used by tracer
		pcCopy                 uint64 // needed for the deferred Tracer
		gasCopy                uint64 // for Tracer to log gas remaining before execution
		callGas                uint64
		logged                 bool   // deferred Tracer should ignore already logged steps
		res                    []byte // result of the opcode execution function
		tracer                 = evm.config.Tracer
		debug                  = tracer != nil && (tracer.OnOpcode != nil || tracer.OnGasChange != nil || tracer.OnFault != nil)
		trace                  = dbg.TraceInstructions && evm.intraBlockState.Trace()
		blockNum               uint64
		txIndex, txIncarnation int
	)

	// Make sure the readOnly is only set if we aren't in readOnly yet.
	// This makes also sure that the readOnly flag isn't removed for child calls.
	restoreReadonly := readOnly && !evm.readOnly
	if restoreReadonly {
		evm.readOnly = true
	}
	// Increment the call depth which is restricted to 1024
	evm.depth++

	// Push this frame onto evm.currentCallContext and link the parent so
	// opSload's chain walk can reach ancestor caches and the deferred
	// rollup can find the parent.
	callContext.parent = evm.currentCallContext
	evm.currentCallContext = callContext

	defer func() {
		// first: capture data/memory/state/depth/etc... then clenup them
		if debug && err != nil {
			if !logged && tracer.OnOpcode != nil {
				tracer.OnOpcode(pcCopy, byte(op), gasCopy, cost, callContext, evm.returnData, evm.depth, VMErrorFromErr(err))
			}
			if logged && tracer.OnFault != nil {
				tracer.OnFault(pcCopy, byte(op), gasCopy, cost, callContext, evm.depth, VMErrorFromErr(err))
			}
		}
		// Slot-cache finaliser.
		//
		// Inner frame (parent != nil):
		//   On success, child's cells propagate to the parent's cache by
		//   pointer copy so the parent inherits a warm post-call view.
		//   On err the snapshot in the caller's opCall rewinds IBS state,
		//   and the child's cache is discarded — parent's cache untouched.
		//
		// Outermost frame (parent == nil):
		//   On success, the cache is the only record of the call's storage
		//   writes (opSstore intentionally skipped ibs.SetState).  Flush
		//   each cell back into IBS so FinalizeTx + parallel-execution
		//   conflict detection see the writes.  On err, drop the cache —
		//   no IBS journal entry was ever made, so nothing to revert.
		if err == nil && len(callContext.slotCache) > 0 {
			if callContext.parent != nil {
				parentCache := callContext.parent.slotCache
				if parentCache == nil {
					parentCache = map[accounts.Address]map[accounts.StorageKey]*state.WriteCell[uint256.Int]{}
					callContext.parent.slotCache = parentCache
				}
				for addr, innerMap := range callContext.slotCache {
					parentInner, ok := parentCache[addr]
					if !ok {
						// Parent had no entries for this address — hand the
						// child's inner map over wholesale.  Child's outer
						// map gets cleared on put() but the inner map
						// survives via the parent's pointer.
						parentCache[addr] = innerMap
						continue
					}
					for k, cell := range innerMap {
						parentInner[k] = cell
					}
				}
			} else {
				ibs := evm.intraBlockState
				for addr, innerMap := range callContext.slotCache {
					for k, cell := range innerMap {
						if ferr := ibs.FlushSlotCacheWrite(addr, k, cell.Value); ferr != nil && err == nil {
							err = ferr
						}
					}
				}
			}
		}
		// Code-cache finaliser.  Same structure as slotCache rollup but
		// read-only (no IBS flush — code is read via IBS.GetCode each
		// time we miss; the cache is purely a per-frame accelerator).
		// On success, child's entries propagate to parent so the parent
		// inherits a warm view of post-call code reads.  On err the
		// child's cache vanishes with the frame; any invalidations the
		// child made on parent/ancestor entries (via invalidateCodeEntry)
		// stay applied, but that's safe — those entries just re-cache
		// from IBS on the next read.
		if err == nil && len(callContext.codeCache) > 0 && callContext.parent != nil {
			parentCodeCache := callContext.parent.codeCache
			if parentCodeCache == nil {
				parentCodeCache = map[accounts.Address]*codeCacheEntry{}
				callContext.parent.codeCache = parentCodeCache
			}
			for addr, entry := range callContext.codeCache {
				parentCodeCache[addr] = entry
			}
		}
		// Codehash-cache rollup mirrors the code-cache rollup above —
		// value-typed map (CodeHash) instead of pointer-to-entry.
		if err == nil && len(callContext.codeHashCache) > 0 && callContext.parent != nil {
			parentCodeHashCache := callContext.parent.codeHashCache
			if parentCodeHashCache == nil {
				parentCodeHashCache = map[accounts.Address]accounts.CodeHash{}
				callContext.parent.codeHashCache = parentCodeHashCache
			}
			for addr, h := range callContext.codeHashCache {
				parentCodeHashCache[addr] = h
			}
		}
		// Nonce-cache rollup: same shape, uint64 values.  Child nonce
		// writes (from CREATE/CREATE2 + SetNonce write-through) propagate
		// up so the parent's view stays consistent with the IBS state
		// the child left behind on success.
		if err == nil && len(callContext.nonceCache) > 0 && callContext.parent != nil {
			parentNonceCache := callContext.parent.nonceCache
			if parentNonceCache == nil {
				parentNonceCache = map[accounts.Address]uint64{}
				callContext.parent.nonceCache = parentNonceCache
			}
			for addr, n := range callContext.nonceCache {
				parentNonceCache[addr] = n
			}
		}
		// Diagnostic dump of frame-local code-cache hit/miss counters at
		// outer-frame return (one log line per tx).  Lets the bench
		// harness confirm the cache is actually being exercised on a
		// given workload.  Tag with `[codecache]` so grep can isolate.
		if callContext.parent == nil {
			if hits, misses := codeCacheHitCount.Load(), codeCacheMissCount.Load(); hits+misses > 0 {
				ratio := float64(hits) * 100 / float64(hits+misses)
				log.Info("[codecache] frame-local cache stats", "hits", hits, "misses", misses, "hit_pct", fmt.Sprintf("%.2f", ratio))
			}
			if hits, misses := codeHashCacheHitCount.Load(), codeHashCacheMissCount.Load(); hits+misses > 0 {
				ratio := float64(hits) * 100 / float64(hits+misses)
				log.Info("[codehashcache] frame-local cache stats", "hits", hits, "misses", misses, "hit_pct", fmt.Sprintf("%.2f", ratio))
			}
			if hits, misses := nonceCacheHitCount.Load(), nonceCacheMissCount.Load(); hits+misses > 0 {
				ratio := float64(hits) * 100 / float64(hits+misses)
				log.Info("[noncecache] frame-local cache stats", "hits", hits, "misses", misses, "hit_pct", fmt.Sprintf("%.2f", ratio))
			}
		}
		// Pop this frame.
		evm.currentCallContext = callContext.parent
		callContext.parent = nil
		// this function must execute _after_: the `CaptureState` needs the stacks before
		callContext.put()
		if restoreReadonly {
			evm.readOnly = false
		}
		evm.depth--
	}()

	// The Interpreter main run loop (contextual). This loop runs until either an
	// explicit STOP, RETURN or SELFDESTRUCT is executed, an error occurred during
	// the execution of one of the operations or until the done flag is set by the
	// parent context.

	var traceGas = func(op OpCode, callGas, cost uint64) uint64 {
		switch op {
		case CALL, CALLCODE, DELEGATECALL, STATICCALL:
			return callGas
		default:
			return cost
		}
	}

	// Hoist to locals so the compiler sees them as loop-invariant.
	isAmsterdam := evm.chainRules.IsAmsterdam
	anyTrace := dbg.TraceDynamicGas || debug || trace

	for {
		callContext.cachedKeyValid = false
		callContext.cachedAddrValid = false
		callContext.cachedSlotCell = nil
		callContext.cachedCodeEntry = nil
		callContext.cachedCodeHashValid = false
		if anyTrace {
			// Capture pre-execution values for tracing.
			logged, pcCopy, gasCopy = false, pc, callContext.gas
			blockNum, txIndex, txIncarnation = evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation()
		}
		// Get the operation from the jump table and validate the stack to ensure there are
		// enough stack items available to perform the operation.
		op = contract.GetOp(pc)
		operation := evm.jt[op]
		cost = operation.constantGas // For tracing
		// Validate stack
		if sLen := callContext.Stack.len(); sLen < operation.numPop {
			return nil, callContext.Gas(), &ErrStackUnderflow{stackLen: sLen, required: operation.numPop}
		} else if sLen > operation.maxStack {
			return nil, callContext.Gas(), &ErrStackOverflow{stackLen: sLen, limit: operation.maxStack}
		}
		// for tracing: this gas consumption event is emitted below in the debug section.
		if callContext.gas < cost {
			return nil, callContext.Gas(), ErrOutOfGas
		} else {
			callContext.gas -= cost
		}
		// EIP-8037: Track constantGas immediately after deduction for block-level accounting.
		if isAmsterdam && cost > 0 {
			evm.regularGasConsumed += cost
		}

		// All ops with a dynamic memory usage also has a dynamic gas cost.
		var memorySize uint64
		if operation.dynamicGas != nil {
			// calculate the new memory size and expand the memory to fit
			// the operation
			// Memory check needs to be done prior to evaluating the dynamic gas portion,
			// to detect calculation overflows
			if operation.memorySize != nil {
				memSize, overflow := operation.memorySize(callContext)
				if overflow {
					return nil, callContext.Gas(), ErrGasUintOverflow
				}
				// memory is expanded in words of 32 bytes. Gas
				// is also calculated in words.
				if memorySize, overflow = math.SafeMul(ToWordSize(memSize), 32); overflow {
					return nil, callContext.Gas(), ErrGasUintOverflow
				}
			}
			// Reset callGasTemp so we can detect if dynamicGas sets it (CALL variants)
			evm.callGasTemp = 0
			// Consume the gas and return an error if not enough gas is available.
			// cost is explicitly set so that the capture state defer method can get the proper cost
			var dynamicCost mdgas.MdGas
			dynamicCost, err = operation.dynamicGas(evm, callContext, callContext.Gas(), memorySize)
			if err != nil {
				if !errors.Is(err, ErrOutOfGas) {
					err = fmt.Errorf("%w: %v", ErrOutOfGas, err)
				}
				return nil, callContext.Gas(), err
			}
			if anyTrace {
				cost += dynamicCost.Regular
				callGas = operation.constantGas + dynamicCost.Regular - evm.CallGasTemp()
				if dbg.TraceDynamicGas && dynamicCost.Regular > 0 {
					fmt.Printf("%d (%d.%d) Dynamic Gas: %d (%s)\n", blockNum, txIndex, txIncarnation, traceGas(op, callGas, cost), op)
				}
			}
			// EIP-8037: "Regular gas charge MUST be applied first. If the regular
			// gas charge triggers an out-of-gas error, the state gas charge is
			// not applied." Deduct regular gas before state gas so that any
			// state-to-regular spill operates on the already-reduced balance.
			if callContext.gas < dynamicCost.Regular {
				return nil, callContext.Gas(), ErrOutOfGas
			}
			callContext.gas -= dynamicCost.Regular
			if isAmsterdam {
				// EIP-8037: Track dynamic regular gas immediately after deduction.
				// For CALL variants, callGasTemp is the gas forwarded to child (escrow),
				// so we subtract it to get parent's actual cost.
				evm.regularGasConsumed += dynamicCost.Regular - evm.CallGasTemp()
			}
			if dynamicCost.State > 0 {
				// Note: do NOT add dynamicCost.State to `cost` here.
				// `cost` is only used for tracing and is compared against `gasCopy`
				// which captures only regular gas. Adding state gas would cause
				// uint64 underflow in the OnGasChange(gasCopy, gasCopy-cost, ...) call below.
				// State gas is charged separately via useMdGas.
				ok := callContext.useMdGas(evm, dynamicCost.State, mdgas.StateGas, nil, tracing.GasChangeIgnored)
				if !ok {
					return nil, callContext.Gas(), ErrOutOfGas
				}
			}
		}

		// Do gas tracing before memory expansion
		if debug {
			if tracer.OnGasChange != nil {
				tracer.OnGasChange(gasCopy, gasCopy-cost, tracing.GasChangeCallOpCode)
			}
			if tracer.OnOpcode != nil {
				tracer.OnOpcode(pc, byte(op), gasCopy, cost, callContext, evm.returnData, evm.depth, VMErrorFromErr(err))
				logged = true
			}
		}

		if memorySize > 0 {
			callContext.Memory.Resize(memorySize)
		}

		// TODO - move this to a trace & set in the worker

		if trace {
			var opstr string
			if operation.string != nil {
				opstr = operation.string(pc, callContext)
			} else {
				opstr = op.String()
			}

			fmt.Printf("%d (%d.%d) %5d %5d %s\n", blockNum, txIndex, txIncarnation, pc, traceGas(op, callGas, cost), opstr)
		}

		// execute the operation
		pc, res, err = operation.execute(pc, evm, callContext)

		if err != nil {
			break
		}
		pc++
	}

	if errors.Is(err, errStopToken) {
		err = nil // clear stop token error
	}

	return res, callContext.Gas(), err
}
