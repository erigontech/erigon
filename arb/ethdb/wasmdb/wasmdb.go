package wasmdb

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/arb/lru"
)

type WasmTarget string

const WasmPrefixLen = 3

// WasmKeyLen = CompiledWasmCodePrefix + moduleHash
const WasmKeyLen = WasmPrefixLen + length.Hash

type WasmPrefix = [WasmPrefixLen]byte
type WasmKey = [WasmKeyLen]byte

const (
	TargetWavm  WasmTarget = "wavm"
	TargetArm64 WasmTarget = "arm64"
	TargetAmd64 WasmTarget = "amd64"
	TargetHost  WasmTarget = "host"
)

var (
	wasmSchemaVersionKey = []byte("WasmSchemaVersion")

	// 0x00 prefix to avoid conflicts when wasmdb is not separate database
	activatedAsmWavmPrefix = WasmPrefix{0x00, 'w', 'w'} // (prefix, moduleHash) -> stylus module (wavm)
	activatedAsmArmPrefix  = WasmPrefix{0x00, 'w', 'r'} // (prefix, moduleHash) -> stylus asm for ARM system
	activatedAsmX86Prefix  = WasmPrefix{0x00, 'w', 'x'} // (prefix, moduleHash) -> stylus asm for x86 system
	activatedAsmHostPrefix = WasmPrefix{0x00, 'w', 'h'} // (prefix, moduleHash) -> stylus asm for system other then ARM and x86
)

const WasmSchemaVersion byte = 0x01

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

func DeprecatedPrefixesV0() (keyPrefixes [][]byte, keyLength int) {
	return [][]byte{
		// deprecated prefixes, used in version 0x00, purged in version 0x01
		{0x00, 'w', 'a'}, // ActivatedAsmPrefix
		{0x00, 'w', 'm'}, // ActivatedModulePrefix
	}, 3 + 32
}

// key = prefix + moduleHash
func activatedKey(prefix WasmPrefix, moduleHash common.Hash) WasmKey {
	var key WasmKey
	copy(key[:WasmPrefixLen], prefix[:])
	copy(key[WasmPrefixLen:], moduleHash[:])
	return key
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

// Stores the activated asm for a given moduleHash and target
func writeActivatedAsm(db kv.Putter, target WasmTarget, moduleHash common.Hash, asm []byte) {
	prefix, err := activatedAsmKeyPrefix(target)
	if err != nil {
		log.Crit("Failed to store activated wasm asm", "err", err)
	}
	key := activatedKey(prefix, moduleHash)
	if err := db.Put(kv.ArbWasmActivationBucket, key[:], asm); err != nil {
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
	asm, err := db.GetOne(kv.ArbWasmActivationBucket, key[:])
	if err != nil {
		return nil
	}
	return asm
}

// Stores wasm schema version
func WriteWasmSchemaVersion(db kv.Putter) {
	if err := db.Put(kv.ArbWasmActivationBucket, wasmSchemaVersionKey, []byte{WasmSchemaVersion}); err != nil {
		log.Crit("Failed to store wasm schema version", "err", err)
	}
}

// Retrieves wasm schema version
func ReadWasmSchemaVersion(db kv.Getter) ([]byte, error) {
	return db.GetOne(kv.ArbWasmActivationBucket, wasmSchemaVersionKey)
}

type WasmIface interface {
	ActivatedAsm(target WasmTarget, moduleHash common.Hash) ([]byte, error)
	WasmStore() kv.RwDB
	WasmCacheTag() uint32
	WasmTargets() []WasmTarget
}

type activatedAsmCacheKey struct {
	moduleHash common.Hash
	target     WasmTarget
}

type WasmDB struct {
	kv.RwDB

	activatedAsmCache *lru.SizeConstrainedCache[activatedAsmCacheKey, []byte]
	cacheTag          uint32
	targets           []WasmTarget
}

func (w *WasmDB) ActivatedAsm(target WasmTarget, moduleHash common.Hash) ([]byte, error) {
	cacheKey := activatedAsmCacheKey{moduleHash, target}
	if asm, _ := w.activatedAsmCache.Get(cacheKey); len(asm) > 0 {
		return asm, nil
	}
	var asm []byte
	err := w.View(context.Background(), func(tx kv.Tx) error {
		asm = ReadActivatedAsm(tx, target, moduleHash)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(asm) > 0 {
		w.activatedAsmCache.Add(cacheKey, asm)
		return asm, nil
	}
	return nil, errors.New("not found")
}

func (w *WasmDB) WriteActivatedAsm(moduleHash common.Hash, asmMap map[WasmTarget][]byte) error {
	return w.Update(context.Background(), func(tx kv.RwTx) error {
		WriteActivation(tx, moduleHash, asmMap)
		return nil
	})
}

func (w *WasmDB) WasmStore() kv.RwDB {
	return w
}

func (w *WasmDB) WasmCacheTag() uint32 {
	return w.cacheTag
}

func (w *WasmDB) WasmTargets() []WasmTarget {
	return w.targets
}

const constantCacheTag = 1

func WrapDatabaseWithWasm(wasm kv.RwDB, targets []WasmTarget) WasmIface {
	return &WasmDB{RwDB: wasm, cacheTag: constantCacheTag, targets: targets, activatedAsmCache: lru.NewSizeConstrainedCache[activatedAsmCacheKey, []byte](1000)}
}

var openedArbitrumWasmDB WasmIface

func OpenArbitrumWasmDB(ctx context.Context, path string) WasmIface {
	if openedArbitrumWasmDB != nil {
		return openedArbitrumWasmDB
	}
	mdbxDB := mdbx.New(kv.ArbWasmDB, log.New()).Path(path).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
			return kv.ChaindataTablesCfg
		}).MustOpen()
	go func() {
		<-ctx.Done()
		openedArbitrumWasmDB = nil
		mdbxDB.Close()
	}()

	openedArbitrumWasmDB = WrapDatabaseWithWasm(mdbxDB, []WasmTarget{LocalTarget()})
	return openedArbitrumWasmDB
}
