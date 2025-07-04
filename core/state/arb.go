package state

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/arb/lru"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/turbo/ethdb/wasmdb"
)

var (
	// Defines prefix bytes for Stylus WASM program bytecode
	// when deployed on-chain via a user-initiated transaction.
	// These byte prefixes are meant to conflict with the L1 contract EOF
	// validation rules so they can be sufficiently differentiated from EVM bytecode.
	// This allows us to store WASM programs as code in the stateDB side-by-side
	// with EVM contracts, but match against these prefix bytes when loading code
	// to execute the WASMs through Stylus rather than the EVM.
	stylusEOFMagic       = byte(0xEF)
	stylusEOFMagicSuffix = byte(0xF0)
	stylusEOFVersion     = byte(0x00)
	// 4th byte specifies the Stylus dictionary used during compression

	StylusDiscriminant = []byte{stylusEOFMagic, stylusEOFMagicSuffix, stylusEOFVersion}
)

func NewArbitrum(ibs *IntraBlockState) IntraBlockStateArbitrum {
	ibs.arbExtraData = &ArbitrumExtraData{
		unexpectedBalanceDelta: new(uint256.Int),
		userWasms:              map[common.Hash]ActivatedWasm{},
		activatedWasms:         map[common.Hash]ActivatedWasm{},
		recentWasms:            NewRecentWasms(),
	}
	return ibs // TODO
}

type IntraBlockStateArbitrum interface {
	evmtypes.IntraBlockState

	// Arbitrum: manage Stylus wasms
	ActivateWasm(moduleHash common.Hash, asmMap map[wasmdb.WasmTarget][]byte)
	TryGetActivatedAsm(target wasmdb.WasmTarget, moduleHash common.Hash) (asm []byte, err error)
	TryGetActivatedAsmMap(targets []wasmdb.WasmTarget, moduleHash common.Hash) (asmMap map[wasmdb.WasmTarget][]byte, err error)
	RecordCacheWasm(wasm CacheWasm)
	RecordEvictWasm(wasm EvictWasm)
	GetRecentWasms() RecentWasms
	UserWasms() UserWasms
	ActivatedAsm(target wasmdb.WasmTarget, moduleHash common.Hash) (asm []byte, err error)
	WasmStore() kv.RwDB
	WasmCacheTag() uint32
	WasmTargets() []wasmdb.WasmTarget

	// Arbitrum: track stylus's memory footprint
	GetStylusPages() (uint16, uint16)
	GetStylusPagesOpen() uint16
	SetStylusPagesOpen(open uint16)
	AddStylusPages(new uint16) (uint16, uint16)
	AddStylusPagesEver(new uint16)

	HasSelfDestructed(addr common.Address) bool

	StartRecording()
	RecordProgram(targets []wasmdb.WasmTarget, moduleHash common.Hash)

	GetStorageRoot(address common.Address) common.Hash
	GetUnexpectedBalanceDelta() *uint256.Int

	// SubscribeNewTxsEvent(ch chan<- core.NewTxsEvent) event.Subscription
	SetArbFinalizer(f func(*ArbitrumExtraData))

	SetTxContext(bn uint64, ti int)
	IntermediateRoot(_ bool) common.Hash
	GetReceiptsByHash(hash common.Hash) types.Receipts
	SetBalance(addr common.Address, amount uint256.Int, reason tracing.BalanceChangeReason) error
	Commit(bn uint64, _ bool) (common.Hash, error)
	FinalizeTx(chainRules *chain.Rules, stateWriter StateWriter) error
	GetLogs(txIndex int, txnHash common.Hash, blockNumber uint64, blockHash common.Hash) types.Logs
	// TxIndex returns the current transaction index set by Prepare.
	TxnIndex() int
	IsTxFiltered() bool
}

func (s *IntraBlockState) IsTxFiltered() bool {
	return s.arbExtraData.arbTxFilter
}

func (s *IntraBlockState) ActivateWasm(moduleHash common.Hash, asmMap map[wasmdb.WasmTarget][]byte) {
	_, exists := s.arbExtraData.activatedWasms[moduleHash]
	if exists {
		return
	}
	s.arbExtraData.activatedWasms[moduleHash] = asmMap
	s.journal.append(wasmActivation{
		moduleHash: moduleHash,
	})
}

func (s *IntraBlockState) TryGetActivatedAsm(target wasmdb.WasmTarget, moduleHash common.Hash) ([]byte, error) {
	asmMap, exists := s.arbExtraData.activatedWasms[moduleHash]
	if exists {
		if asm, exists := asmMap[target]; exists {
			return asm, nil
		}
	}
	return s.ActivatedAsm(target, moduleHash)
}

func (s *IntraBlockState) TryGetActivatedAsmMap(targets []wasmdb.WasmTarget, moduleHash common.Hash) (map[wasmdb.WasmTarget][]byte, error) {
	asmMap := s.arbExtraData.activatedWasms[moduleHash]
	if asmMap != nil {
		for _, target := range targets {
			if _, exists := asmMap[target]; !exists {
				return nil, fmt.Errorf("newly activated wasms for module %v exist, but they don't contain asm for target %v", moduleHash, target)
			}
		}
		return asmMap, nil
	}
	var err error
	asmMap = make(map[wasmdb.WasmTarget][]byte, len(targets))
	for _, target := range targets {
		asm, dbErr := s.ActivatedAsm(target, moduleHash)
		if dbErr == nil {
			asmMap[target] = asm
		} else {
			err = errors.Join(fmt.Errorf("failed to read activated asm from database for target %v and module %v: %w", target, moduleHash, dbErr), err)
		}
	}
	return asmMap, err
}

func (s *IntraBlockState) GetStylusPages() (uint16, uint16) {
	return s.arbExtraData.openWasmPages, s.arbExtraData.everWasmPages
}

func (s *IntraBlockState) GetStylusPagesOpen() uint16 {
	return s.arbExtraData.openWasmPages
}

func (s *IntraBlockState) SetStylusPagesOpen(open uint16) {
	s.arbExtraData.openWasmPages = open
}

// Tracks that `new` additional pages have been opened, returning the previous counts
func (s *IntraBlockState) AddStylusPages(new uint16) (uint16, uint16) {
	open, ever := s.GetStylusPages()
	s.arbExtraData.openWasmPages = common.SaturatingUAdd(open, new)
	s.arbExtraData.everWasmPages = max(ever, s.arbExtraData.openWasmPages)
	return open, ever
}

// TODO arbitrum - not used in og nitro as well
func (s *IntraBlockState) AddStylusPagesEver(new uint16) {
	s.arbExtraData.everWasmPages = common.SaturatingUAdd(s.arbExtraData.everWasmPages, new)
}

var ErrArbTxFilter error = errors.New("internal error")

type ArbitrumExtraData struct {
	unexpectedBalanceDelta *uint256.Int                  // total balance change across all accounts
	userWasms              UserWasms                     // user wasms encountered during execution
	openWasmPages          uint16                        // number of pages currently open
	everWasmPages          uint16                        // largest number of pages ever allocated during this tx's execution
	activatedWasms         map[common.Hash]ActivatedWasm // newly activated WASMs
	recentWasms            RecentWasms
	arbTxFilter            bool
}

func (s *IntraBlockState) SetArbFinalizer(f func(*ArbitrumExtraData)) {
	runtime.SetFinalizer(s.arbExtraData, f)
}

func (s *IntraBlockState) GetCurrentTxLogs() []types.Logs {
	return s.logs
	//return s.logs[s.thash]
}

// GetUnexpectedBalanceDelta returns the total unexpected change in balances since the last commit to the database.
func (s *IntraBlockState) GetUnexpectedBalanceDelta() *uint256.Int {
	return s.arbExtraData.unexpectedBalanceDelta
}

func (s *IntraBlockState) GetSelfDestructs() []common.Address {
	selfDestructs := []common.Address{}
	for addr := range s.journal.dirties {
		obj, exist := s.stateObjects[addr]
		if !exist {
			continue
		}
		if obj.selfdestructed {
			selfDestructs = append(selfDestructs, addr)
		}
	}
	return selfDestructs
}

func (sdb *IntraBlockState) ActivatedAsm(target wasmdb.WasmTarget, moduleHash common.Hash) (asm []byte, err error) {
	if sdb.wasmDB == nil {
		panic("IBS: wasmDB not set")
	}
	return sdb.wasmDB.ActivatedAsm(target, moduleHash)
}

func (sdb *IntraBlockState) WasmStore() kv.RwDB {
	if sdb.wasmDB == nil {
		panic("IBS: wasmDB not set")
	}
	//TODO implement me
	return sdb.wasmDB.WasmStore()
}

func (sdb *IntraBlockState) WasmCacheTag() uint32 {
	if sdb.wasmDB == nil {
		panic("IBS: wasmDB not set")
	}
	return sdb.wasmDB.WasmCacheTag()
}

func (sdb *IntraBlockState) WasmTargets() []wasmdb.WasmTarget {
	if sdb.wasmDB == nil {
		panic("IBS: wasmDB not set")
	}
	return sdb.wasmDB.WasmTargets()
}

func (sdb *IntraBlockState) GetReceiptsByHash(hash common.Hash) types.Receipts {
	return nil
	//TODO implement me
	panic("implement me")
}

func (sdb *IntraBlockState) Commit(bn uint64, _ bool) (common.Hash, error) {
	return common.Hash{}, nil
	//TODO implement me
	panic("implement me")
}

// making the function public to be used by external tests
// func ForEachStorage(s *IntraBlockState, addr common.Address, cb func(key, value common.Hash) bool) error {
// 	return forEachStorage(s, addr, cb)
// }

// moved here from statedb_test.go
// func forEachStorage(s *IntraBlockState, addr common.Address, cb func(key, value common.Hash) bool) error {
// 	s.domains.IterateStoragePrefix(addr[:], cb)
// 	so := s.getStateObject(addr)
// 	if so == nil {
// 		return nil
// 	}
// 	tr, err := so.getTrie()
// 	if err != nil {
// 		return err
// 	}
// 	trieIt, err := tr.NodeIterator(nil)
// 	if err != nil {
// 		return err
// 	}
// 	it := trie.NewIterator(trieIt)

// 	for it.Next() {
// 		key := common.BytesToHash(s.trie.GetKey(it.Key))
// 		if value, dirty := so.dirtyStorage[key]; dirty {
// 			if !cb(key, value) {
// 				return nil
// 			}
// 			continue
// 		}

// 		if len(it.Value) > 0 {
// 			_, content, _, err := rlp.Split(it.Value)
// 			if err != nil {
// 				return err
// 			}
// 			if !cb(key, common.BytesToHash(content)) {
// 				return nil
// 			}
// 		}
// 	}
// 	return nil
// }

// maps moduleHash to activation info
type UserWasms map[common.Hash]ActivatedWasm

func (s *IntraBlockState) StartRecording() {
	s.arbExtraData.userWasms = make(UserWasms)
}

func (s *IntraBlockState) RecordProgram(targets []wasmdb.WasmTarget, moduleHash common.Hash) {
	if len(targets) == 0 {
		// nothing to record
		return
	}
	asmMap, err := s.TryGetActivatedAsmMap(targets, moduleHash)
	if err != nil {
		log.Crit("can't find activated wasm while recording", "modulehash", moduleHash, "err", err)
	}
	if s.arbExtraData.userWasms != nil {
		s.arbExtraData.userWasms[moduleHash] = asmMap
	}
}

func (s *IntraBlockState) UserWasms() UserWasms {
	return s.arbExtraData.userWasms
}

func (s *IntraBlockState) RecordCacheWasm(wasm CacheWasm) {
	s.journal.entries = append(s.journal.entries, wasm)
}

func (s *IntraBlockState) RecordEvictWasm(wasm EvictWasm) {
	s.journal.entries = append(s.journal.entries, wasm)
}

func (s *IntraBlockState) GetRecentWasms() RecentWasms {
	return s.arbExtraData.recentWasms
}

func (s *IntraBlockState) HasSelfDestructed(addr common.Address) bool {
	stateObject, err := s.getStateObject(addr)
	if err != nil {
		panic(err)
	}
	if stateObject != nil {
		return stateObject.selfdestructed
	}
	return false
}

func (s *IntraBlockState) IntermediateRoot(deleteEmptyObjects bool) common.Hash {
	_, fn, ln, _ := runtime.Caller(1)
	log.Warn("need shared domains and writer to calculate intermediate root", "caller", fmt.Sprintf("%s:%d", fn, ln))
	return common.Hash{}
}

// GetStorageRoot retrieves the storage root from the given address or empty
// if object not found.
func (s *IntraBlockState) GetStorageRoot(addr common.Address) common.Hash {
	stateObject, err := s.getStateObject(addr)
	if err == nil && stateObject != nil {
		return stateObject.data.Root
	}
	return common.Hash{}
}

func (sdb *IntraBlockState) SetWasmDB(wasmDB wasmdb.WasmIface) {
	sdb.wasmDB = wasmDB
}
func (sdb *IntraBlockState) ExpectBalanceBurn(amount *uint256.Int) {
	if amount.Sign() < 0 {
		panic(fmt.Sprintf("ExpectBalanceBurn called with negative amount %v", amount))
	}
	sdb.arbExtraData.unexpectedBalanceDelta.Add(sdb.arbExtraData.unexpectedBalanceDelta, amount)
}

type ActivatedWasm map[wasmdb.WasmTarget][]byte

// checks if a valid Stylus prefix is present
func IsStylusProgram(b []byte) bool {
	if len(b) < len(StylusDiscriminant)+1 {
		return false
	}
	return bytes.Equal(b[:3], StylusDiscriminant)
}

// strips the Stylus header from a contract, returning the dictionary used
func StripStylusPrefix(b []byte) ([]byte, byte, error) {
	if !IsStylusProgram(b) {
		return nil, 0, errors.New("specified bytecode is not a Stylus program")
	}
	return b[4:], b[3], nil
}

// creates a new Stylus prefix from the given dictionary byte
func NewStylusPrefix(dictionary byte) []byte {
	prefix := bytes.Clone(StylusDiscriminant)
	return append(prefix, dictionary)
}

type wasmActivation struct {
	moduleHash common.Hash
}

func (ch wasmActivation) revert(s *IntraBlockState) error {
	delete(s.arbExtraData.activatedWasms, ch.moduleHash)
	return nil
}

func (ch wasmActivation) dirtied() *common.Address {
	return nil
}

// Updates the Rust-side recent program cache
var CacheWasmRust func(asm []byte, moduleHash common.Hash, version uint16, tag uint32, debug bool) = func([]byte, common.Hash, uint16, uint32, bool) {}
var EvictWasmRust func(moduleHash common.Hash, version uint16, tag uint32, debug bool) = func(common.Hash, uint16, uint32, bool) {}

type CacheWasm struct {
	ModuleHash common.Hash
	Version    uint16
	Tag        uint32
	Debug      bool
}

func (ch CacheWasm) revert(*IntraBlockState) error {
	EvictWasmRust(ch.ModuleHash, ch.Version, ch.Tag, ch.Debug)
	return nil
}

func (ch CacheWasm) dirtied() *common.Address {
	return nil
}

type EvictWasm struct {
	ModuleHash common.Hash
	Version    uint16
	Tag        uint32
	Debug      bool
}

func (ch EvictWasm) revert(s *IntraBlockState) error {
	asm, err := s.TryGetActivatedAsm(wasmdb.LocalTarget(), ch.ModuleHash) // only happens in native mode
	if err == nil && len(asm) != 0 {
		//if we failed to get it - it's not in the current rust cache
		CacheWasmRust(asm, ch.ModuleHash, ch.Version, ch.Tag, ch.Debug)
	}
	return err
}

func (ch EvictWasm) dirtied() *common.Address {
	return nil
}

// Type for managing recent program access.
// The cache contained is discarded at the end of each block.
type RecentWasms struct {
	cache *lru.BasicLRU[common.Hash, struct{}]
}

// Creates an un uninitialized cache
func NewRecentWasms() RecentWasms {
	return RecentWasms{cache: nil}
}

// Inserts a new item, returning true if already present.
func (p RecentWasms) Insert(item common.Hash, retain uint16) bool {
	if p.cache == nil {
		cache := lru.NewBasicLRU[common.Hash, struct{}](int(retain))
		p.cache = &cache
	}
	if _, hit := p.cache.Get(item); hit {
		return hit
	}
	p.cache.Add(item, struct{}{})
	return false
}

// Copies all entries into a new LRU.
func (p RecentWasms) Copy() RecentWasms {
	if p.cache == nil {
		return NewRecentWasms()
	}
	cache := lru.NewBasicLRU[common.Hash, struct{}](p.cache.Capacity())
	for _, item := range p.cache.Keys() {
		cache.Add(item, struct{}{})
	}
	return RecentWasms{cache: &cache}
}
