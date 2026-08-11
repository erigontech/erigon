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
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func (evm *EVM) precompile(addr accounts.Address) (PrecompiledContract, bool) {
	// Precompiled contracts can be overridden, otherwise determine the active set based on chain rules
	precompiles := evm.precompiles
	if precompiles == nil {
		precompiles = Precompiles(evm.chainRules)
	}
	p, ok := precompiles[addr]
	return p, ok
}

// EVM is the Ethereum Virtual Machine base object and provides
// the necessary tools to run a contract on the given state with
// the provided context. It should be noted that any error
// generated through any of the calls should be considered a
// revert-state-and-consume-all-gas operation, no checks on
// specific errors should ever be performed. The interpreter makes
// sure that any errors generated are to be considered faulty code.
//
// The EVM should never be reused and is not thread safe.
type EVM struct {
	// Context provides auxiliary blockchain related information
	Context evmtypes.BlockContext
	evmtypes.TxContext
	// IntraBlockState gives access to the underlying state
	intraBlockState *state.IntraBlockState

	// table holds the opcode specific handlers
	jt *JumpTable

	// depth is the current call stack
	depth int

	// chainConfig contains information about the current chain
	chainConfig *chain.Config
	// chain rules contains the chain rules for the current epoch
	chainRules *chain.Rules
	// virtual machine configuration options used to initialise the
	// evm.
	config Config
	// abort is used to abort the EVM calling operations
	abort atomic.Bool
	// callGasTemp holds the gas available for the current call. This is needed because the
	// available gas is calculated in gasCall* according to the 63/64 rule and later
	// applied in opCall*.
	callGasTemp uint64
	// optional overridden set of precompiled contracts
	precompiles PrecompiledContracts

	readOnly   bool   // Whether to throw on stateful modifications
	returnData []byte // Last CALL's return data for subsequent reuse

	// Pointers before counters: interleaving them adds a word of padding.
	internCache *storageKeyCache
	addrCache   *addressCache
	internOps   uint32
	addrOps     uint32
}

// evmSizeClass is the Go allocation size class EVM fills exactly. One more word
// moves every EVM into the 480-byte class, so a field added here has to either
// pack into existing padding or accept that cost knowingly.
const evmSizeClass = 448

var _ [0]struct{} = [unsafe.Sizeof(EVM{}) - evmSizeClass]struct{}{}

// storageKeyCacheSize must comfortably exceed a contract's live slot count,
// or conflict misses dominate.
const storageKeyCacheSize = 1024

// storageKeyCacheMinOps delays the table until interning has cost more than
// zeroing 40KB: a hit saves ~16ns, so an EVM resolving fewer keys than this
// cannot win the allocation back however well the keys repeat.
const storageKeyCacheMinOps = 128

// slotIndex masks rather than divides, so the size has to be a power of two.
var _ [0]struct{} = [storageKeyCacheSize & (storageKeyCacheSize - 1)]struct{}{}

// storageKeyCache memoizes InternKey by the source stack word. Interning is a
// pure function and a live handle keeps its entry alive, so the cache never
// goes stale and is never cleared. Words are stored beside the handles because
// that is cheaper than recovering them via handle.Value(); the pointer-free
// words go last so the GC scan stops at the handles.
type storageKeyCache struct {
	handles [storageKeyCacheSize]accounts.StorageKey
	words   [storageKeyCacheSize]uint256.Int
}

// slotIndex mixes all four limbs: keccak-derived slots differ across the whole
// word, array and scalar slots only in the lowest.
func slotIndex(word *uint256.Int) uint64 {
	return (word[0] ^ word[1] ^ word[2] ^ word[3]) & (storageKeyCacheSize - 1)
}

// A nil handle marks an unused entry; the zero word is a legitimate key.
func (c *storageKeyCache) fill(i uint64, word *uint256.Int) accounts.StorageKey {
	h := accounts.InternKey(word.Bytes32())
	c.words[i], c.handles[i] = *word, h
	return h
}

// internStorageKey returns word interned as a StorageKey, skipping unique.Make
// for words seen before. Short-lived EVMs intern uncached: the table only earns
// back its allocation over a few hundred storage ops.
func (evm *EVM) internStorageKey(word *uint256.Int) accounts.StorageKey {
	c := evm.internCache
	if c == nil {
		if evm.internOps < storageKeyCacheMinOps {
			evm.internOps++
			return accounts.InternKey(word.Bytes32())
		}
		c = new(storageKeyCache)
		evm.internCache = c
		return c.fill(slotIndex(word), word)
	}
	i := slotIndex(word)
	if h := c.handles[i]; h != accounts.NilKey && c.words[i] == *word {
		return h
	}
	return c.fill(i, word)
}

// Address streams are far narrower than storage-key streams — a handful of
// routers and tokens dominate — so the address table is a quarter of the size
// and wins its zeroing back sooner.
const (
	addressCacheSize   = 256
	addressCacheMinOps = 32
)

var _ [0]struct{} = [addressCacheSize & (addressCacheSize - 1)]struct{}{}

// addressCache is storageKeyCache for InternAddress; see that type for why the
// entries never go stale and why the words sit beside the handles.
type addressCache struct {
	handles [addressCacheSize]accounts.Address
	words   [addressCacheSize]uint256.Int
}

// addrIndex mixes only what Bytes20 reads: limbs 0 and 1 plus the low half of
// limb 2. A stack word may carry anything above the address, and that dirt must
// reach neither the index nor the compare — the bucket is masked to the low
// bits, so a dirty word lands on its clean twin's entry, where a whole-word
// compare would have the two evict each other on every access.
func addrIndex(word *uint256.Int) uint64 {
	return (word[0] ^ word[1] ^ uint64(uint32(word[2]))) & (addressCacheSize - 1)
}

// fill clears the bits above the address, so the entry hits for every form of
// the word carrying it.
func (c *addressCache) fill(i uint64, word *uint256.Int) accounts.Address {
	h := accounts.InternAddress(word.Bytes20())
	c.words[i] = uint256.Int{word[0], word[1], uint64(uint32(word[2])), 0}
	c.handles[i] = h
	return h
}

// internAddress returns the low 20 bytes of word interned as an Address,
// skipping unique.Make for words seen before. One entry serves an address in
// every form its word can take.
func (evm *EVM) internAddress(word *uint256.Int) accounts.Address {
	c := evm.addrCache
	if c == nil {
		if evm.addrOps < addressCacheMinOps {
			evm.addrOps++
			return accounts.InternAddress(word.Bytes20())
		}
		c = new(addressCache)
		evm.addrCache = c
	}
	i := addrIndex(word)
	if h := c.handles[i]; h != accounts.NilAddress && c.words[i][0] == word[0] &&
		c.words[i][1] == word[1] && c.words[i][2] == uint64(uint32(word[2])) {
		return h
	}
	return c.fill(i, word)
}

// NewEVM returns a new EVM. The returned EVM is not thread safe and should
// only ever be used *once*.
func NewEVM(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, ibs *state.IntraBlockState, chainConfig *chain.Config, vmConfig Config) *EVM {
	if vmConfig.NoBaseFee {
		if txCtx.GasPrice.IsZero() {
			blockCtx.BaseFee = uint256.Int{}
		}
	}
	evm := &EVM{
		Context:         blockCtx,
		TxContext:       txCtx,
		intraBlockState: ibs,
		config:          vmConfig,
		chainConfig:     chainConfig,
		chainRules:      blockCtx.Rules(chainConfig),
	}
	evm.jt = jumpTable(evm.chainRules, vmConfig)

	return evm
}

// Reset resets the EVM with a new transaction context.Reset
// This is not threadsafe and should only be done very cautiously.
func (evm *EVM) Reset(txCtx evmtypes.TxContext, ibs *state.IntraBlockState) {
	evm.TxContext = txCtx
	evm.intraBlockState = ibs

	// ensure the evm is reset to be used again
	evm.abort.Store(false)
}

func (evm *EVM) ResetBetweenBlocks(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, ibs *state.IntraBlockState, vmConfig Config, chainRules *chain.Rules) {
	if vmConfig.NoBaseFee {
		if txCtx.GasPrice.IsZero() {
			blockCtx.BaseFee = uint256.Int{}
		}
	}
	evm.Context = blockCtx
	evm.TxContext = txCtx
	evm.intraBlockState = ibs
	evm.config = vmConfig
	evm.chainRules = chainRules

	evm.depth = 0
	evm.returnData = nil
	evm.jt = jumpTable(chainRules, vmConfig)

	// ensure the evm is reset to be used again
	evm.abort.Store(false)
}

// Cancel cancels any running EVM operation. This may be called concurrently and
// it's safe to be called multiple times.
func (evm *EVM) Cancel() { evm.abort.Store(true) }

// Cancelled returns true if Cancel has been called
func (evm *EVM) Cancelled() bool { return evm.abort.Load() }

func (evm *EVM) handleFrameRevert(gasRemaining *mdgas.MdGas, err error, snapshot int, entryStateReservoir uint64, stateGasSpill uint64) {
	evm.intraBlockState.RevertToSnapshot(snapshot, err)
	if evm.chainRules.IsAmsterdam {
		gasRemaining.Execution += stateGasSpill
		gasRemaining.State = entryStateReservoir
	}
	if err != ErrExecutionReverted {
		if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
			evm.config.Tracer.OnGasChange(gasRemaining.Execution, 0, tracing.GasChangeCallFailedExecution)
		}
		gasRemaining.Execution = 0
	}
}

// deriveFrameExecutionGasUsed derives the execution-gas component of a frame's
// gasUsed from the total gas it received (inputTotal), the total left over
// at exit (gasRemainingTotal), and the frame's signed net state-gas usage.
//
// Execution = (input − leftover) − state. Computed in uint64 modular
// arithmetic: a negative stateGasUsed becomes a large uint64 via the cast,
// and subtracting it wraps mod 2^64 into the correct positive sum. Safe at
// any gas magnitude.
func deriveFrameExecutionGasUsed(inputTotal, gasRemainingTotal uint64, stateGasUsed int64) uint64 {
	return inputTotal - gasRemainingTotal - uint64(stateGasUsed)
}

// CallGasTemp returns the callGasTemp for the EVM
func (evm *EVM) CallGasTemp() uint64 {
	return evm.callGasTemp
}

// SetCallGasTemp sets the callGasTemp for the EVM
func (evm *EVM) SetCallGasTemp(gas uint64) {
	evm.callGasTemp = gas
}

func isSystemCall(caller accounts.Address) bool {
	return caller == params.SystemAddress
}

// SetPrecompiles sets the precompiles for the EVM
func (evm *EVM) SetPrecompiles(precompiles PrecompiledContracts) {
	evm.precompiles = precompiles
}

func (evm *EVM) call(typ OpCode, caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int, bailout bool) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	if evm.abort.Load() {
		return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, nil
	}

	depth := evm.depth
	gasRemaining = gas
	inputTotal := gas.Total()
	defer func() {
		gasUsed.Execution = deriveFrameExecutionGasUsed(inputTotal, gasRemaining.Total(), gasUsed.State)
	}()

	version := evm.intraBlockState.Version()
	if (dbg.TraceTransactionIO && !dbg.TraceInstructions) && (evm.intraBlockState.Trace() || dbg.TraceAccount(caller.Handle())) {
		fmt.Printf("%d (%d.%d) %s: %x %x\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, typ, addr, input)
		defer func() {
			fmt.Printf("%d (%d.%d) RETURN (%s): %x: %x, %d, %v\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, typ, addr, ret, gasRemaining, err)
		}()
	}

	p, isPrecompile := evm.precompile(addr)
	var code []byte
	if !isPrecompile {
		code, err = evm.intraBlockState.ResolveCode(addr)
		if err != nil {
			return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
	}

	// Invoke tracer hooks that signal entering/exiting a call frame
	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller, addr, isPrecompile, input, gas, value, code)
		defer func(startGas mdgas.MdGas) {
			evm.captureEnd(depth, typ, startGas, gasRemaining, ret, err)
		}(gas)
	}

	// BAL: record address access even if call fails due to gas/call depth/insufficient balance
	evm.intraBlockState.MarkAddressAccess(addr, false)

	if evm.config.NoRecursion && depth > 0 {
		return nil, gasRemaining, mdgas.MdGasUsage{}, nil
	}
	// Fail if we're trying to execute above the call depth limit
	if depth > int(params.CallCreateDepth) {
		return nil, gasRemaining, mdgas.MdGasUsage{}, ErrDepth
	}
	syscall := isSystemCall(caller)

	if typ == CALL || typ == CALLCODE {
		// Fail if we're trying to transfer more than the available balance.
		// Skip the check for zero-value calls, matching geth's short-circuit.
		if !value.IsZero() {
			canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller, value)
			if err != nil {
				return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, err
			}
			if !canTransfer && !bailout {
				return nil, gasRemaining, mdgas.MdGasUsage{}, ErrInsufficientBalance
			}
		}
	}

	snapshot := evm.intraBlockState.PushSnapshot()
	defer evm.intraBlockState.PopSnapshot(snapshot)

	if typ == CALL {
		exist, err := evm.intraBlockState.Exist(addr)
		if err != nil {
			return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if !exist {
			// Under EIP-161, a zero-value CALL to a non-existent
			// non-precompile account short-circuits as a no-op instead of
			// creating the account. This also preserves the EIP-4788
			// beacon-root syscall's "no-op when not deployed" semantics at
			// the fork-transition block, before the contract is deployed.
			if !isPrecompile && evm.chainRules.IsEIP161Enabled() && value.IsZero() {
				return nil, gasRemaining, mdgas.MdGasUsage{}, nil
			}
			evm.intraBlockState.CreateAccount(addr, false)
		}
		// System calls use TouchAccount instead of Transfer to avoid
		// spurious balance reads on the caller that would pollute the
		// Block Access List (EIP-7928). The touch is still needed so
		// AuRa/Gnosis keeps the empty system account in the PMT.
		if syscall && value.IsZero() {
			if err := evm.intraBlockState.TouchAccount(caller); err != nil {
				return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
			}
		} else {
			// Normal (non-syscall) calls always go through Transfer —
			// this handles both value movement and the zero-balance touch
			// required for state clearing.
			if err := evm.Context.Transfer(evm.intraBlockState, caller, addr, value, bailout, evm.chainRules); err != nil {
				return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
			}
		}
	} else if typ == STATICCALL {
		// Trigger a touch on the callee so EIP-161 state clearing applies to
		// empty accounts (matters on test networks; on Mainnet all empties are
		// gone by Byzantium). Use TouchAccount rather than AddBalance(0): the
		// latter has a serial-mode shortcut for the RIPEMD-160 precompile
		// (special-snowflake balance-increase path) that bypasses
		// GetOrNewStateObject. Without loading the account the FinalizeTx
		// "exists in dirties but not stateObjects → skip" branch fires and
		// the touch never reaches state-clearing — diverging from
		// CALL's behavior, which loads the account via Exist() before the
		// zero-value Transfer. Affects ethereum/tests RevertPrecompiledTouch_d3.
		if err := evm.intraBlockState.TouchAccount(addr); err != nil {
			return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
	}

	// It is allowed to call precompiles, even via delegatecall
	switch {
	case isPrecompile:
		ret, gasRemaining.Execution, err = RunPrecompiledContract(p, input, gasRemaining.Execution, evm.Config().Tracer)
	case len(code) == 0:
		// If the account has no code, we can abort here
		// The depth-check is already done, and precompiles handled above
		ret, err = nil, nil // gas is unchanged
	default:
		// Initialise a new contract and set the code that is to be used by the EVM.
		// The contract is a scoped environment for this execution context only.
		var codeHash accounts.CodeHash
		codeHash, err = evm.intraBlockState.ResolveCodeHash(addr)
		if err != nil {
			return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		var contract Contract
		switch typ {
		case CALLCODE:
			contract = Contract{
				caller:   caller,
				addr:     caller,
				value:    value,
				Code:     code,
				CodeHash: codeHash,
			}
		case DELEGATECALL:
			contract = Contract{
				caller:   callerAddress,
				addr:     caller,
				value:    value,
				Code:     code,
				CodeHash: codeHash,
			}
		default:
			contract = Contract{
				caller:   caller,
				addr:     addr,
				value:    value,
				Code:     code,
				CodeHash: codeHash,
			}
		}
		readOnly := false
		if typ == STATICCALL {
			readOnly = true
		}
		ret, gasRemaining, gasUsed, err = evm.Run(contract, gasRemaining, input, readOnly)
	}
	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in Homestead this also counts for code storage gas errors.
	if err != nil || evm.config.RestoreState {
		evm.handleFrameRevert(&gasRemaining, err, snapshot, gas.State, gasUsed.StateSpill)
	}

	return ret, gasRemaining, gasUsed, err
}

// Call executes the contract associated with the addr with the given input as
// parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
func (evm *EVM) Call(caller accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int, bailout bool) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	return evm.call(CALL, caller, caller, addr, input, gas, value, bailout)
}

// CallCode executes the contract associated with the addr with the given input
// as parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
//
// CallCode differs from Call in the sense that it executes the given address'
// code with the caller as context.
func (evm *EVM) CallCode(caller accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	return evm.call(CALLCODE, caller, caller, addr, input, gas, value, false)
}

// DelegateCall executes the contract associated with the addr with the given input
// as parameters. It reverses the state in case of an execution error.
//
// DelegateCall differs from CallCode in the sense that it executes the given address'
// code with the caller as context and the caller is set to the caller of the caller.
func (evm *EVM) DelegateCall(caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, input []byte, value uint256.Int, gas mdgas.MdGas) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	return evm.call(DELEGATECALL, caller, callerAddress, addr, input, gas, value, false)
}

// StaticCall executes the contract associated with the addr with the given input
// as parameters while disallowing any modifications to the state during the call.
// Opcodes that attempt to perform such modifications will result in exceptions
// instead of performing the modifications.
func (evm *EVM) StaticCall(caller accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	return evm.call(STATICCALL, caller, caller, addr, input, gas, uint256.Int{}, false)
}

type codeAndHash struct {
	code []byte
	hash accounts.CodeHash
}

func NewCodeAndHash(code []byte) *codeAndHash {
	return &codeAndHash{code: code}
}

func (c *codeAndHash) Hash() accounts.CodeHash {
	if c.hash.IsZero() {
		c.hash = accounts.InternCodeHash(crypto.Keccak256Hash(c.code))
	}
	return c.hash
}

type createPreparation struct {
	callerNonce          uint64
	chargeNewAccount     bool
	incrementCallerNonce bool
}

func (evm *EVM) prepareCreate(caller accounts.Address, address accounts.Address, value uint256.Int, incrementNonce bool, bailout bool, nested bool) (createPreparation, error) {
	var preparation createPreparation
	if evm.depth > int(params.CallCreateDepth) {
		return preparation, ErrDepth
	}
	canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller, value)
	if err != nil {
		return preparation, err
	}
	if !canTransfer && !bailout {
		return preparation, ErrInsufficientBalance
	}
	if incrementNonce {
		preparation.callerNonce, err = evm.intraBlockState.GetNonce(caller)
		if err != nil {
			return preparation, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if preparation.callerNonce+1 < preparation.callerNonce {
			return preparation, ErrNonceUintOverflow
		}
		if nested {
			preparation.incrementCallerNonce = true
		} else {
			evm.intraBlockState.SetNonce(caller, preparation.callerNonce+1, tracing.NonceChangeContractCreator)
		}
	}
	if evm.chainRules.IsBerlin {
		evm.intraBlockState.AddAddressToAccessList(address)
	}
	evm.intraBlockState.MarkAddressAccess(address, false)
	if evm.chainRules.IsAmsterdam && nested {
		preparation.chargeNewAccount, err = evm.intraBlockState.Empty(address)
		if err != nil {
			return preparation, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
	}
	return preparation, nil
}

func (evm *EVM) hasCreateCollision(address accounts.Address) (bool, error) {
	targetCodeHash, err := evm.intraBlockState.GetCodeHash(address)
	if err != nil {
		return false, err
	}
	targetNonce, err := evm.intraBlockState.GetNonce(address)
	if err != nil {
		return false, err
	}
	targetHasStorage, err := evm.intraBlockState.HasStorage(address)
	if err != nil {
		return false, err
	}
	return targetNonce != 0 || !targetCodeHash.IsEmpty() || targetHasStorage, nil
}

func (evm *EVM) OverlayCreate(caller accounts.Address, codeAndHash *codeAndHash, gas mdgas.MdGas, value uint256.Int, address accounts.Address, typ OpCode, incrementNonce bool) ([]byte, accounts.Address, mdgas.MdGas, mdgas.MdGasUsage, error) {
	ret, addr, gasRemaining, gasUsed, err := evm.create(caller, codeAndHash, gas, value, address, typ, incrementNonce, false)
	return ret, addr, gasRemaining, gasUsed, err
}

// create creates a new contract using code as deployment code.
func (evm *EVM) create(caller accounts.Address, codeAndHash *codeAndHash, gas mdgas.MdGas, value uint256.Int, address accounts.Address, typ OpCode, incrementNonce bool, bailout bool) ([]byte, accounts.Address, mdgas.MdGas, mdgas.MdGasUsage, error) {
	return evm.createWithPreparation(caller, codeAndHash, gas, value, address, typ, incrementNonce, bailout, nil)
}

func (evm *EVM) createPrepared(caller accounts.Address, codeAndHash *codeAndHash, gas mdgas.MdGas, value uint256.Int, address accounts.Address, typ OpCode, preparation createPreparation) ([]byte, accounts.Address, mdgas.MdGas, mdgas.MdGasUsage, error) {
	return evm.createWithPreparation(caller, codeAndHash, gas, value, address, typ, false, false, &preparation)
}

func (evm *EVM) createWithPreparation(caller accounts.Address, codeAndHash *codeAndHash, gas mdgas.MdGas, value uint256.Int, address accounts.Address, typ OpCode, incrementNonce bool, bailout bool, preparation *createPreparation) (ret []byte, createAddress accounts.Address, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	gasRemaining = gas

	if dbg.TraceTransactionIO && (evm.intraBlockState.Trace() || dbg.TraceAccount(caller.Handle())) {
		defer func() {
			version := evm.intraBlockState.Version()
			if err != nil {
				fmt.Printf("%d (%d.%d) Create Contract: %x, err=%s\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, createAddress, err)
			} else {
				fmt.Printf("%d (%d.%d) Create Contract: %x, gas=%d\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, createAddress, gasRemaining)
			}
		}()
	}

	depth := evm.depth
	inputTotal := gas.Total()
	defer func() {
		gasUsed.Execution = deriveFrameExecutionGasUsed(inputTotal, gasRemaining.Total(), gasUsed.State)
	}()

	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller, address, false, codeAndHash.code, gas, value, nil)
		defer func() {
			evm.captureEnd(depth, typ, gas, gasRemaining, ret, err)
		}()
	}

	if preparation == nil {
		var prepared createPreparation
		prepared, err = evm.prepareCreate(caller, address, value, incrementNonce, bailout, false)
		if err != nil {
			if err != ErrDepth && err != ErrInsufficientBalance && err != ErrNonceUintOverflow {
				gasRemaining = mdgas.MdGas{}
			}
			return
		}
		preparation = &prepared
	}
	if preparation.incrementCallerNonce {
		evm.intraBlockState.SetNonce(caller, preparation.callerNonce+1, tracing.NonceChangeContractCreator)
	}
	var collision bool
	collision, err = evm.hasCreateCollision(address)
	if err != nil {
		gasRemaining = mdgas.MdGas{}
		err = fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		return
	}
	if collision {
		err = ErrContractAddressCollision
		if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
			evm.Config().Tracer.OnGasChange(gasRemaining.Execution, 0, tracing.GasChangeCallFailedExecution)
		}
		return nil, accounts.NilAddress, mdgas.MdGas{State: gas.State}, mdgas.MdGasUsage{}, err
	}
	// Create a new account on the state
	snapshot := evm.intraBlockState.PushSnapshot()
	defer evm.intraBlockState.PopSnapshot(snapshot)

	evm.intraBlockState.CreateAccount(address, true)
	if evm.chainRules.IsEIP161Enabled() {
		evm.intraBlockState.SetNonce(address, 1, tracing.NonceChangeNewContract)
	}
	if err := evm.Context.Transfer(evm.intraBlockState, caller, address, value, bailout, evm.chainRules); err != nil {
		return nil, accounts.NilAddress, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}

	// Initialise a new contract and set the code that is to be used by the EVM.
	// The contract is a scoped environment for this execution context only.
	contract := Contract{
		caller:   caller,
		addr:     address,
		value:    value,
		Code:     codeAndHash.code,
		CodeHash: codeAndHash.hash,
	}

	if evm.config.NoRecursion && depth > 0 {
		return nil, address, gasRemaining, mdgas.MdGasUsage{}, nil
	}

	ret, gasRemaining, gasUsed, err = evm.Run(contract, gasRemaining, nil, false)

	// EIP-170: Contract code size limit
	if err == nil {
		err = CheckMaxCodeSize(len(ret), evm.chainRules)
	}
	// Reject code starting with 0xEF if EIP-3541 is enabled.
	if err == nil && evm.chainRules.IsLondon && len(ret) >= 1 && ret[0] == 0xEF {
		err = ErrInvalidCode
	}
	// If the contract creation ran successfully and no errors were returned,
	// calculate the gas required to store the code. If the code could not
	// be stored due to not enough gas, set an error when we're in Homestead and let it be handled
	// by the error checking condition below.
	if err == nil {
		// EIP-8037: GAS_CODE_DEPOSIT = cpsb/byte (state) + 6*ceil(len/32) (execution)
		// Pre-Amsterdam: GAS_CODE_DEPOSIT = 200/byte (execution only)
		preDepositGas := gasRemaining

		// Charge state gas (Amsterdam only).
		stateGasOk := true
		var stateGas, depositStateSpill uint64
		if evm.chainRules.IsAmsterdam {
			stateGas = uint64(len(ret)) * params.CostPerStateByte
			gasRemaining, depositStateSpill, stateGasOk = useMdGas(gasRemaining, stateGas, mdgas.StateGas, evm.Config().Tracer, tracing.GasChangeCallCodeStorage)
		}

		// Charge execution gas.
		var executionGasOk bool
		if stateGasOk {
			var executionGas uint64
			if evm.chainRules.IsAmsterdam {
				// EIP-8037 "Contract deployment cost calculation", success path:
				// HASH_COST(L) = 6*ceil(L/32); the state component (cpsb*L) is charged above.
				executionGas = params.Keccak256WordGas * ToWordSize(uint64(len(ret)))
			} else {
				executionGas = uint64(len(ret)) * params.CreateDataGas
			}
			gasRemaining, _, executionGasOk = useMdGas(gasRemaining, executionGas, mdgas.ExecutionGas, evm.Config().Tracer, tracing.GasChangeCallCodeStorage)
		}

		if stateGasOk && executionGasOk {
			evm.intraBlockState.SetCode(address, ret, tracing.CodeChangeContractCreation)
			// EIP-8037: post-Run code-deposit state charge counts toward this
			// frame's state-gas usage; its spilled portion propagates so an
			// ancestor revert refills it from the right pool.
			gasUsed.State += int64(stateGas)
			gasUsed.StateSpill += depositStateSpill
		} else {
			if evm.chainRules.IsAmsterdam {
				// Code deposit failed: per EIP-8037 the failure cost is
				// GAS_CREATE + initcode_execution_cost only; code deposit
				// gas (both state and execution) is excluded.
				gasRemaining = preDepositGas
			}
			// If we run out of gas, we do not store the code: the returned code must be empty.
			ret = []byte{}
			if evm.chainRules.IsHomestead {
				err = ErrCodeStoreOutOfGas
			}
		}
	}

	// When an error was returned by the EVM or when setting the creation code
	// above, we revert to the snapshot and consume any gas remaining. Additionally,
	// when we're in Homestead, this also counts for code storage gas errors.
	if err != nil && (evm.chainRules.IsHomestead || err != ErrCodeStoreOutOfGas) {
		evm.handleFrameRevert(&gasRemaining, err, snapshot, gas.State, gasUsed.StateSpill)
	}

	return ret, address, gasRemaining, gasUsed, err
}

// Create creates a new contract using code as deployment code.
// If salt is non-nil, CREATE2 addressing is used (keccak256(0xff ++ msg.sender ++ salt ++ keccak256(init_code))[12:]);
// otherwise the usual sender-and-nonce-hash is used (CREATE).
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (evm *EVM) Create(caller accounts.Address, code []byte, gas mdgas.MdGas, endowment uint256.Int, salt *uint256.Int, bailout bool) (ret []byte, contractAddr accounts.Address, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	ch := &codeAndHash{code: code}
	op := CREATE
	if salt != nil {
		op = CREATE2
		contractAddr = accounts.InternAddress(types.CreateAddress2(caller.Value(), salt.Bytes32(), ch.Hash()))
	} else {
		var nonce uint64
		nonce, err = evm.intraBlockState.GetNonce(caller)
		if err != nil {
			return nil, accounts.NilAddress, mdgas.MdGas{}, mdgas.MdGasUsage{}, err
		}
		contractAddr = accounts.InternAddress(types.CreateAddress(caller.Value(), nonce))
	}
	ret, contractAddr, gasRemaining, gasUsed, err = evm.create(caller, ch, gas, endowment, contractAddr, op, true /* incrementNonce */, bailout)
	return
}

// SysCreate is a special (system) contract creation methods for genesis constructors.
// Unlike the normal Create & Create2, it doesn't increment caller's nonce.
func (evm *EVM) SysCreate(caller accounts.Address, code []byte, gas mdgas.MdGas, endowment uint256.Int, contractAddr accounts.Address) (ret []byte, gasRemaining mdgas.MdGas, err error) {
	ret, _, gasRemaining, _, err = evm.create(caller, &codeAndHash{code: code}, gas, endowment, contractAddr, CREATE, false /* incrementNonce */, false)
	return
}

// Config returns the environment's chain configuration
func (evm *EVM) Config() Config {
	return evm.config
}

// ChainConfig returns the environment's chain configuration
func (evm *EVM) ChainConfig() *chain.Config {
	return evm.chainConfig
}

// ChainRules returns the environment's chain rules
func (evm *EVM) ChainRules() *chain.Rules {
	return evm.chainRules
}

// IntraBlockState returns the EVM's IntraBlockState
func (evm *EVM) IntraBlockState() *state.IntraBlockState {
	return evm.intraBlockState
}

// GetVMContext provides context about the block being executed as well as state
// to the tracers.
func (evm *EVM) GetVMContext() *tracing.VMContext {
	return &tracing.VMContext{
		Coinbase:        evm.Context.Coinbase,
		BlockNumber:     evm.Context.BlockNumber,
		Time:            evm.Context.Time,
		Random:          evm.Context.PrevRanDao,
		GasPrice:        evm.TxContext.GasPrice,
		ChainConfig:     evm.ChainConfig(),
		IntraBlockState: evm.IntraBlockState(),
		TxHash:          evm.TxHash,
	}
}

func (evm *EVM) captureBegin(depth int, typ OpCode, from accounts.Address, to accounts.Address, precompile bool, input []byte, startGas mdgas.MdGas, value uint256.Int, code []byte) {
	tracer := evm.Config().Tracer

	if tracer.OnEnter != nil {
		tracer.OnEnter(depth, byte(typ), from, to, precompile, input, startGas.Execution, value, code)
	}
	if tracer.OnGasChange != nil {
		tracer.OnGasChange(0, startGas.Execution, tracing.GasChangeCallInitialBalance)
	}
}

func (evm *EVM) captureEnd(depth int, typ OpCode, startGas mdgas.MdGas, leftOverGas mdgas.MdGas, ret []byte, err error) {
	tracer := evm.Config().Tracer

	if leftOverGas.Execution != 0 && tracer.OnGasChange != nil {
		tracer.OnGasChange(leftOverGas.Execution, 0, tracing.GasChangeCallLeftOverReturned)
	}

	var reverted bool
	if err != nil {
		reverted = true
	}
	if !evm.chainRules.IsHomestead && errors.Is(err, ErrCodeStoreOutOfGas) {
		reverted = false
	}

	if tracer.OnExit != nil {
		tracer.OnExit(depth, ret, startGas.Execution-leftOverGas.Execution, VMErrorFromErr(err), reverted)
	}
}
