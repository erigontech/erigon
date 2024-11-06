package state

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/lru"
	"github.com/erigontech/erigon/arb/ethdb"
	"math/big"
	"runtime"
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

type ActivatedWasm map[ethdb.WasmTarget][]byte

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

func (s *StateDB) ActivateWasm(moduleHash common.Hash, asmMap map[ethdb.WasmTarget][]byte) {
	_, exists := s.arbExtraData.activatedWasms[moduleHash]
	if exists {
		return
	}
	s.arbExtraData.activatedWasms[moduleHash] = asmMap
	s.journal.append(wasmActivation{
		moduleHash: moduleHash,
	})
}

func (s *StateDB) TryGetActivatedAsm(target ethdb.WasmTarget, moduleHash common.Hash) ([]byte, error) {
	asmMap, exists := s.arbExtraData.activatedWasms[moduleHash]
	if exists {
		if asm, exists := asmMap[target]; exists {
			return asm, nil
		}
	}
	return s.db.ActivatedAsm(target, moduleHash)
}

func (s *StateDB) TryGetActivatedAsmMap(targets []ethdb.WasmTarget, moduleHash common.Hash) (map[ethdb.WasmTarget][]byte, error) {
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
	asmMap = make(map[ethdb.WasmTarget][]byte, len(targets))
	for _, target := range targets {
		asm, dbErr := s.db.ActivatedAsm(target, moduleHash)
		if dbErr == nil {
			asmMap[target] = asm
		} else {
			err = errors.Join(fmt.Errorf("failed to read activated asm from database for target %v and module %v: %w", target, moduleHash, dbErr), err)
		}
	}
	return asmMap, err
}

func (s *StateDB) GetStylusPages() (uint16, uint16) {
	return s.arbExtraData.openWasmPages, s.arbExtraData.everWasmPages
}

func (s *StateDB) GetStylusPagesOpen() uint16 {
	return s.arbExtraData.openWasmPages
}

func (s *StateDB) SetStylusPagesOpen(open uint16) {
	s.arbExtraData.openWasmPages = open
}

// Tracks that `new` additional pages have been opened, returning the previous counts
func (s *StateDB) AddStylusPages(new uint16) (uint16, uint16) {
	open, ever := s.GetStylusPages()
	s.arbExtraData.openWasmPages = common.SaturatingUAdd(open, new)
	s.arbExtraData.everWasmPages = common.MaxInt(ever, s.arbExtraData.openWasmPages)
	return open, ever
}

func (s *StateDB) AddStylusPagesEver(new uint16) {
	s.arbExtraData.everWasmPages = common.SaturatingUAdd(s.arbExtraData.everWasmPages, new)
}

func NewDeterministic(root common.Hash, db Database) (*StateDB, error) {
	sdb, err := New(root, db, nil)
	if err != nil {
		return nil, err
	}
	sdb.deterministic = true
	return sdb, nil
}

func (s *StateDB) Deterministic() bool {
	return s.deterministic
}

type ArbitrumExtraData struct {
	unexpectedBalanceDelta *big.Int                      // total balance change across all accounts
	userWasms              UserWasms                     // user wasms encountered during execution
	openWasmPages          uint16                        // number of pages currently open
	everWasmPages          uint16                        // largest number of pages ever allocated during this tx's execution
	activatedWasms         map[common.Hash]ActivatedWasm // newly activated WASMs
	recentWasms            RecentWasms
}

func (s *StateDB) SetArbFinalizer(f func(*ArbitrumExtraData)) {
	runtime.SetFinalizer(s.arbExtraData, f)
}

func (s *StateDB) GetCurrentTxLogs() []*types.Log {
	return s.logs[s.thash]
}

// GetUnexpectedBalanceDelta returns the total unexpected change in balances since the last commit to the database.
func (s *StateDB) GetUnexpectedBalanceDelta() *big.Int {
	return new(big.Int).Set(s.arbExtraData.unexpectedBalanceDelta)
}

func (s *StateDB) GetSelfDestructs() []common.Address {
	selfDestructs := []common.Address{}
	for addr := range s.journal.dirties {
		obj, exist := s.stateObjects[addr]
		if !exist {
			continue
		}
		if obj.selfDestructed {
			selfDestructs = append(selfDestructs, addr)
		}
	}
	return selfDestructs
}

// making the function public to be used by external tests
func ForEachStorage(s *StateDB, addr common.Address, cb func(key, value common.Hash) bool) error {
	return forEachStorage(s, addr, cb)
}

// moved here from statedb_test.go
func forEachStorage(s *StateDB, addr common.Address, cb func(key, value common.Hash) bool) error {
	so := s.getStateObject(addr)
	if so == nil {
		return nil
	}
	tr, err := so.getTrie()
	if err != nil {
		return err
	}
	trieIt, err := tr.NodeIterator(nil)
	if err != nil {
		return err
	}
	it := trie.NewIterator(trieIt)

	for it.Next() {
		key := common.BytesToHash(s.trie.GetKey(it.Key))
		if value, dirty := so.dirtyStorage[key]; dirty {
			if !cb(key, value) {
				return nil
			}
			continue
		}

		if len(it.Value) > 0 {
			_, content, _, err := rlp.Split(it.Value)
			if err != nil {
				return err
			}
			if !cb(key, common.BytesToHash(content)) {
				return nil
			}
		}
	}
	return nil
}

// maps moduleHash to activation info
type UserWasms map[common.Hash]ActivatedWasm

func (s *StateDB) StartRecording() {
	s.arbExtraData.userWasms = make(UserWasms)
}

func (s *StateDB) RecordProgram(targets []ethdb.WasmTarget, moduleHash common.Hash) {
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

func (s *StateDB) UserWasms() UserWasms {
	return s.arbExtraData.userWasms
}

func (s *StateDB) RecordCacheWasm(wasm CacheWasm) {
	s.journal.entries = append(s.journal.entries, wasm)
}

func (s *StateDB) RecordEvictWasm(wasm EvictWasm) {
	s.journal.entries = append(s.journal.entries, wasm)
}

func (s *StateDB) GetRecentWasms() RecentWasms {
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
