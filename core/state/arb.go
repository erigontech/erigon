package state

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/common/lru"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/holiman/uint256"
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

type ActivatedWasm map[WasmTarget][]byte

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

type WasmTarget string

const (
	TargetWavm  WasmTarget = "wavm"
	TargetArm64 WasmTarget = "arm64"
	TargetAmd64 WasmTarget = "amd64"
	TargetHost  WasmTarget = "host"
)

func LocalTarget() WasmTarget {
	if runtime.GOOS == "linux" {
		switch runtime.GOARCH {
		case "arm64":
			return TargetArm64
		case "amd64":
			return TargetAmd64
		}
	}
	return TargetHost
}

func activatedAsmKeyPrefix(target WasmTarget) (WasmPrefix, error) {
	var prefix WasmPrefix
	switch target {
	case TargetWavm:
		prefix = activatedAsmWavmPrefix
	case TargetArm64:
		prefix = activatedAsmArmPrefix
	case TargetAmd64:
		prefix = activatedAsmX86Prefix
	case TargetHost:
		prefix = activatedAsmHostPrefix
	default:
		return WasmPrefix{}, fmt.Errorf("invalid target: %v", target)
	}
	return prefix, nil
}

func IsSupportedWasmTarget(target WasmTarget) bool {
	_, err := activatedAsmKeyPrefix(target)
	return err == nil
}

func WriteActivation(db kv.Putter, moduleHash common.Hash, asmMap map[WasmTarget][]byte) {
	for target, asm := range asmMap {
		writeActivatedAsm(db, target, moduleHash, asm)
	}
}

const WasmActivationTable = "wasm_activation"

// Stores the activated asm for a given moduleHash and target
func writeActivatedAsm(db kv.Putter, target WasmTarget, moduleHash common.Hash, asm []byte) {
	prefix, err := activatedAsmKeyPrefix(target)
	if err != nil {
		log.Crit("Failed to store activated wasm asm", "err", err)
	}
	key := activatedKey(prefix, moduleHash)
	if err := db.Put(WasmActivationTable, key[:], asm); err != nil {
		log.Crit("Failed to store activated wasm asm", "err", err)
	}
}

// Retrieves the activated asm for a given moduleHash and target
func ReadActivatedAsm(db kv.Getter, target WasmTarget, moduleHash common.Hash) []byte {
	prefix, err := activatedAsmKeyPrefix(target)
	if err != nil {
		log.Crit("Failed to read activated wasm asm", "err", err)
	}
	key := activatedKey(prefix, moduleHash)
	asm, err := db.GetOne(WasmActivationTable, key[:])
	if err != nil {
		return nil
	}
	return asm
}

// Stores wasm schema version
func WriteWasmSchemaVersion(db kv.Putter) {
	if err := db.Put(WasmActivationTable, wasmSchemaVersionKey, []byte{WasmSchemaVersion}); err != nil {
		log.Crit("Failed to store wasm schema version", "err", err)
	}
}

// Retrieves wasm schema version
func ReadWasmSchemaVersion(db kv.Getter) ([]byte, error) {
	return db.GetOne(WasmActivationTable, wasmSchemaVersionKey)
}

const WasmSchemaVersion byte = 0x01

const WasmPrefixLen = 3

// WasmKeyLen = CompiledWasmCodePrefix + moduleHash
const WasmKeyLen = WasmPrefixLen + length.Hash

type WasmPrefix = [WasmPrefixLen]byte
type WasmKey = [WasmKeyLen]byte

var (
	wasmSchemaVersionKey = []byte("WasmSchemaVersion")

	// 0x00 prefix to avoid conflicts when wasmdb is not separate database
	activatedAsmWavmPrefix = WasmPrefix{0x00, 'w', 'w'} // (prefix, moduleHash) -> stylus module (wavm)
	activatedAsmArmPrefix  = WasmPrefix{0x00, 'w', 'r'} // (prefix, moduleHash) -> stylus asm for ARM system
	activatedAsmX86Prefix  = WasmPrefix{0x00, 'w', 'x'} // (prefix, moduleHash) -> stylus asm for x86 system
	activatedAsmHostPrefix = WasmPrefix{0x00, 'w', 'h'} // (prefix, moduleHash) -> stylus asm for system other then ARM and x86
)

func DeprecatedPrefixesV0() (keyPrefixes [][]byte, keyLength int) {
	return [][]byte{
		// deprecated prefixes, used in version 0x00, purged in version 0x01
		[]byte{0x00, 'w', 'a'}, // ActivatedAsmPrefix
		[]byte{0x00, 'w', 'm'}, // ActivatedModulePrefix
	}, 3 + 32
}

// key = prefix + moduleHash
func activatedKey(prefix WasmPrefix, moduleHash common.Hash) WasmKey {
	var key WasmKey
	copy(key[:WasmPrefixLen], prefix[:])
	copy(key[WasmPrefixLen:], moduleHash[:])
	return key
}

type IntraBlockStateArbitrum interface {
	evmtypes.IntraBlockState

	// Arbitrum: manage Stylus wasms
	ActivateWasm(moduleHash common.Hash, asmMap map[WasmTarget][]byte)
	TryGetActivatedAsm(target WasmTarget, moduleHash common.Hash) (asm []byte, err error)
	TryGetActivatedAsmMap(targets []WasmTarget, moduleHash common.Hash) (asmMap map[WasmTarget][]byte, err error)
	RecordCacheWasm(wasm CacheWasm)
	RecordEvictWasm(wasm EvictWasm)
	GetRecentWasms() RecentWasms

	// Arbitrum: track stylus's memory footprint
	GetStylusPages() (uint16, uint16)
	GetStylusPagesOpen() uint16
	SetStylusPagesOpen(open uint16)
	AddStylusPages(new uint16) (uint16, uint16)
	AddStylusPagesEver(new uint16)

	HasSelfDestructed(addr common.Address) bool
}

func (s *IntraBlockState) ActivateWasm(moduleHash common.Hash, asmMap map[WasmTarget][]byte) {
	_, exists := s.arbExtraData.activatedWasms[moduleHash]
	if exists {
		return
	}
	s.arbExtraData.activatedWasms[moduleHash] = asmMap
	s.journal.append(wasmActivation{
		moduleHash: moduleHash,
	})
}

func (s *IntraBlockState) TryGetActivatedAsm(target WasmTarget, moduleHash common.Hash) ([]byte, error) {
	asmMap, exists := s.arbExtraData.activatedWasms[moduleHash]
	if exists {
		if asm, exists := asmMap[target]; exists {
			return asm, nil
		}
	}
	// return s.ActivatedAsm(target, moduleHash)
	return nil, errors.New("not found")
}

func (s *IntraBlockState) TryGetActivatedAsmMap(targets []WasmTarget, moduleHash common.Hash) (map[WasmTarget][]byte, error) {
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
	asmMap = make(map[WasmTarget][]byte, len(targets))
	for _, target := range targets {
		// asm, dbErr := s.db.ActivatedAsm(target, moduleHash)
		asm := []byte{}
		var dbErr error
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

func (s *IntraBlockState) AddStylusPagesEver(new uint16) {
	s.arbExtraData.everWasmPages = common.SaturatingUAdd(s.arbExtraData.everWasmPages, new)
}

type ArbitrumExtraData struct {
	unexpectedBalanceDelta *uint256.Int                  // total balance change across all accounts
	userWasms              UserWasms                     // user wasms encountered during execution
	openWasmPages          uint16                        // number of pages currently open
	everWasmPages          uint16                        // largest number of pages ever allocated during this tx's execution
	activatedWasms         map[common.Hash]ActivatedWasm // newly activated WASMs
	recentWasms            RecentWasms
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

//// making the function public to be used by external tests
//func ForEachStorage(s *IntraBlockState, addr common.Address, cb func(key, value common.Hash) bool) error {
//	return forEachStorage(s, addr, cb)
//}
//
//// moved here from statedb_test.go
//func forEachStorage(s *IntraBlockState, addr common.Address, cb func(key, value common.Hash) bool) error {
//	s.domains.IterateStoragePrefix(addr[:], cb)
//	so := s.getStateObject(addr)
//	if so == nil {
//		return nil
//	}
//	tr, err := so.getTrie()
//	if err != nil {
//		return err
//	}
//	trieIt, err := tr.NodeIterator(nil)
//	if err != nil {
//		return err
//	}
//	it := trie.NewIterator(trieIt)
//
//	for it.Next() {
//		key := common.BytesToHash(s.trie.GetKey(it.Key))
//		if value, dirty := so.dirtyStorage[key]; dirty {
//			if !cb(key, value) {
//				return nil
//			}
//			continue
//		}
//
//		if len(it.Value) > 0 {
//			_, content, _, err := rlp.Split(it.Value)
//			if err != nil {
//				return err
//			}
//			if !cb(key, common.BytesToHash(content)) {
//				return nil
//			}
//		}
//	}
//	return nil
//}

// maps moduleHash to activation info
type UserWasms map[common.Hash]ActivatedWasm

func (s *IntraBlockState) StartRecording() {
	s.arbExtraData.userWasms = make(UserWasms)
}

func (s *IntraBlockState) RecordProgram(targets []WasmTarget, moduleHash common.Hash) {
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
