package dbutils

import (
	"encoding/binary"

	"github.com/ledgerwatch/turbo-geth/common"
)

// EncodeBlockNumber encodes a block number as big endian uint64
func EncodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

// headerKey = headerPrefix + num (uint64 big endian) + hash
func HeaderKey(number uint64, hash common.Hash) []byte {
	return append(EncodeBlockNumber(number), hash.Bytes()...)
}

// headerTDKey = headerPrefix + num (uint64 big endian) + hash + headerTDSuffix
func HeaderTDKey(number uint64, hash common.Hash) []byte {
	return append(HeaderKey(number, hash), HeaderTDSuffix...)
}

// headerHashKey = headerPrefix + num (uint64 big endian) + headerHashSuffix
func HeaderHashKey(number uint64) []byte {
	return append(EncodeBlockNumber(number), HeaderHashSuffix...)
}

// headerNumberKey = headerNumberPrefix + hash
func HeaderNumberKey(hash common.Hash) []byte {
	return append(HeaderNumberPrefix, hash.Bytes()...)
}

// blockBodyKey = blockBodyPrefix + num (uint64 big endian) + hash
func BlockBodyKey(number uint64, hash common.Hash) []byte {
	return append(EncodeBlockNumber(number), hash.Bytes()...)
}

// blockReceiptsKey = blockReceiptsPrefix + num (uint64 big endian) + hash
func BlockReceiptsKey(number uint64, hash common.Hash) []byte {
	return append(EncodeBlockNumber(number), hash.Bytes()...)
}

// txLookupKey = txLookupPrefix + hash
func TxLookupKey(hash common.Hash) []byte {
	return append(TxLookupPrefix, hash.Bytes()...)
}

// bloomBitsKey = bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash
func BloomBitsKey(bit uint, section uint64, hash common.Hash) []byte {
	key := append(make([]byte, 10), hash.Bytes()...)

	binary.BigEndian.PutUint16(key[0:], uint16(bit))
	binary.BigEndian.PutUint64(key[2:], section)

	return key
}

// preimageKey = preimagePrefix + hash
func PreimageKey(hash common.Hash) []byte {
	return append(PreimagePrefix, hash.Bytes()...)
}

// configKey = configPrefix + hash
func ConfigKey(hash common.Hash) []byte {
	return append(ConfigPrefix, hash.Bytes()...)
}

func GenerateCompositeTrieKey(addressHash common.Hash, seckey common.Hash) []byte {
	compositeKey := make([]byte, 0, common.HashLength+common.HashLength)
	compositeKey = append(compositeKey, addressHash[:]...)
	compositeKey = append(compositeKey, seckey[:]...)
	return compositeKey
}
func GenerateCompositeStorageKey(addressHash common.Hash, incarnation uint64, seckey common.Hash) []byte {
	compositeKey := make([]byte, 0, common.HashLength+8+common.HashLength)
	compositeKey = append(compositeKey, GenerateStoragePrefix(addressHash, incarnation)...)
	compositeKey = append(compositeKey, seckey[:]...)
	return compositeKey
}
func GenerateStoragePrefix(addressHash common.Hash, incarnation uint64) []byte {
	prefix := make([]byte, 0, common.HashLength+8)
	prefix = append(prefix, addressHash[:]...)

	//todo pool
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, incarnation^^uint64(0))
	prefix = append(prefix, buf...)
	return prefix
}

func CompositeKeySuffix(key []byte, timestamp uint64) (composite, suffix []byte) {
	suffix = EncodeTimestamp(timestamp)
	composite = make([]byte, len(key)+len(suffix))
	copy(composite, key)
	copy(composite[len(key):], suffix)
	return composite, suffix
}
