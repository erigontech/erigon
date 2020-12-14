package dbutils

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/stretchr/testify/assert"
)

func TestHeaderTypeDetection(t *testing.T) {

	// good input
	headerHashKey := common.Hex2Bytes("00000000000000006e")
	assert.False(t, IsHeaderKey(headerHashKey))
	assert.False(t, IsHeaderTDKey(headerHashKey))
	assert.True(t, IsHeaderHashKey(headerHashKey))

	headerKey := common.Hex2Bytes("0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd")
	assert.True(t, IsHeaderKey(headerKey))
	assert.False(t, IsHeaderTDKey(headerKey))
	assert.False(t, IsHeaderHashKey(headerKey))

	headerTdKey := common.Hex2Bytes("0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd74")
	assert.False(t, IsHeaderKey(headerTdKey))
	assert.True(t, IsHeaderTDKey(headerTdKey))
	assert.False(t, IsHeaderHashKey(headerTdKey))

	// bad input
	emptyKey := common.Hex2Bytes("")
	assert.False(t, IsHeaderKey(emptyKey))
	assert.False(t, IsHeaderTDKey(emptyKey))
	assert.False(t, IsHeaderHashKey(emptyKey))

	tooLongKey := common.Hex2Bytes("0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd")
	assert.False(t, IsHeaderKey(tooLongKey))
	assert.False(t, IsHeaderTDKey(tooLongKey))
	assert.False(t, IsHeaderHashKey(tooLongKey))

	notRelatedInput := common.Hex2Bytes("alex")
	assert.False(t, IsHeaderKey(notRelatedInput))
	assert.False(t, IsHeaderTDKey(notRelatedInput))
	assert.False(t, IsHeaderHashKey(notRelatedInput))

}

func TestPlainParseStoragePrefix(t *testing.T) {
	expectedAddr := common.HexToAddress("0x5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c")
	expectedIncarnation := uint64(999000999)

	prefix := PlainGenerateStoragePrefix(expectedAddr[:], expectedIncarnation)

	addr, incarnation := PlainParseStoragePrefix(prefix)

	assert.Equal(t, expectedAddr, addr, "address should be extracted")
	assert.Equal(t, expectedIncarnation, incarnation, "incarnation should be extracted")
}

func TestPlainParseCompositeStorageKey(t *testing.T) {
	expectedAddr := common.HexToAddress("0x5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c")
	expectedIncarnation := uint64(999000999)
	expectedKey := common.HexToHash("0x58833f949125129fb8c6c93d2c6003c5bab7c0b116d695f4ca137b1debf4e472")

	compositeKey := PlainGenerateCompositeStorageKey(expectedAddr.Bytes(), expectedIncarnation, expectedKey.Bytes())

	addr, incarnation, key := PlainParseCompositeStorageKey(compositeKey)

	assert.Equal(t, expectedAddr, addr, "address should be extracted")
	assert.Equal(t, expectedIncarnation, incarnation, "incarnation should be extracted")
	assert.Equal(t, expectedKey, key, "key should be extracted")
}

func TestParseStoragePrefix(t *testing.T) {
	expectedAddrHash, _ := common.HashData(common.HexToAddress("0x5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c").Bytes())
	expectedIncarnation := uint64(999000999)

	prefix := GenerateStoragePrefix(expectedAddrHash[:], expectedIncarnation)

	addrHash, incarnation := ParseStoragePrefix(prefix)

	assert.Equal(t, expectedAddrHash, addrHash, "address should be extracted")
	assert.Equal(t, expectedIncarnation, incarnation, "incarnation should be extracted")
}

func TestParseCompositeStorageKey(t *testing.T) {
	expectedAddrHash, _ := common.HashData(common.HexToAddress("0x5A0b54D5dc17e0AadC383d2db43B0a0D3E029c4c").Bytes())
	expectedIncarnation := uint64(999000999)
	expectedKey := common.HexToHash("0x58833f949125129fb8c6c93d2c6003c5bab7c0b116d695f4ca137b1debf4e472")

	compositeKey := GenerateCompositeStorageKey(expectedAddrHash, expectedIncarnation, expectedKey)

	addrHash, incarnation, key := ParseCompositeStorageKey(compositeKey)

	assert.Equal(t, expectedAddrHash, addrHash, "address should be extracted")
	assert.Equal(t, expectedIncarnation, incarnation, "incarnation should be extracted")
	assert.Equal(t, expectedKey, key, "key should be extracted")
}
