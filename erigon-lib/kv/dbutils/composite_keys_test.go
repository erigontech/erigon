package dbutils

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
)

func TestPlainParseStoragePrefix(t *testing.T) {
	expectedAddr := libcommon.HexToAddress("0x5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c")
	expectedIncarnation := uint64(999000999)

	prefix := PlainGenerateStoragePrefix(expectedAddr[:], expectedIncarnation)

	addr, incarnation := PlainParseStoragePrefix(prefix)

	assert.Equal(t, expectedAddr, addr, "address should be extracted")
	assert.Equal(t, expectedIncarnation, incarnation, "incarnation should be extracted")
}

func TestPlainParseCompositeStorageKey(t *testing.T) {
	expectedAddr := libcommon.HexToAddress("0x5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c")
	expectedIncarnation := uint64(999000999)
	expectedKey := libcommon.HexToHash("0x58833f949125129fb8c6c93d2c6003c5bab7c0b116d695f4ca137b1debf4e472")

	compositeKey := PlainGenerateCompositeStorageKey(expectedAddr.Bytes(), expectedIncarnation, expectedKey.Bytes())

	addr, incarnation, key := PlainParseCompositeStorageKey(compositeKey)

	assert.Equal(t, expectedAddr, addr, "address should be extracted")
	assert.Equal(t, expectedIncarnation, incarnation, "incarnation should be extracted")
	assert.Equal(t, expectedKey, key, "key should be extracted")
}

func TestParseStoragePrefix(t *testing.T) {
	expectedAddrHash, _ := libcommon.HashData(libcommon.HexToAddress("0x5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c").Bytes())
	expectedIncarnation := uint64(999000999)

	prefix := GenerateStoragePrefix(expectedAddrHash[:], expectedIncarnation)

	addrHash, incarnation := ParseStoragePrefix(prefix)

	assert.Equal(t, expectedAddrHash, addrHash, "address should be extracted")
	assert.Equal(t, expectedIncarnation, incarnation, "incarnation should be extracted")
}

func TestParseCompositeStorageKey(t *testing.T) {
	expectedAddrHash, _ := common.HashData(libcommon.HexToAddress("0x5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c").Bytes())
	expectedIncarnation := uint64(999000999)
	expectedKey := libcommon.HexToHash("0x58833f949125129fb8c6c93d2c6003c5bab7c0b116d695f4ca137b1debf4e472")

	compositeKey := GenerateCompositeStorageKey(expectedAddrHash, expectedIncarnation, expectedKey)

	addrHash, incarnation, key := ParseCompositeStorageKey(compositeKey)

	assert.Equal(t, expectedAddrHash, addrHash, "address should be extracted")
	assert.Equal(t, expectedIncarnation, incarnation, "incarnation should be extracted")
	assert.Equal(t, expectedKey, key, "key should be extracted")
}
