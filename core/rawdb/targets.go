package rawdb

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
)

var CodePrefix = []byte("c") // CodePrefix + code hash -> account code

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
