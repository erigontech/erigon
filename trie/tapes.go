package trie

import (
	"io"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
)

// BytesTape is an abstraction for an input tape that allows reading binary strings ([]byte) sequentially
// To be used for keys and binary string values
type BytesTape interface {
	// Returned slice is only valid until the next invocation of NextBytes()
	// i.e. the underlying array/slice may be shared between invocations
	Next() ([]byte, error)
}

// Uint64Tape is an abstraction for an input tape that allows reading unsigned 64-bit integers sequentially
// To be used for nonces of the accounts
type Uint64Tape interface {
	Next() (uint64, error)
}

// BigIntTape is an abstraction for an input tape that allows reading *big.Int values sequentially
// To be used for balances of the accounts
type BigIntTape interface {
	// Returned pointer is only valid until the next invocation of NextBitInt()
	// i.e. the underlying big.Int object may be shared between invocation
	Next() (*big.Int, error)
}

// HashTape is an abstraction for an input table that allows reading 32-byte hashes (common.Hash) sequentially
// To be used for intermediate hashes in the Patricia Merkle tree
type HashTape interface {
	Next() (common.Hash, error)
}

// RlpSerializableTape is an abstraction for a tape that allows reading a sequence of
// RlpSerializable values.
type RlpSerializableTape interface {
	Next() (RlpSerializable, error)
}

// RlpSerializable is a value that can be double-RLP coded.
type RlpSerializable interface {
	ToDoubleRLP(io.Writer) error
	DoubleRLPLen() int
	RawBytes() []byte
}

// RlpSerializableBytesTape is a wrapper on top of BytesTape that wraps every return value
// into RlpSerializableBytes, treating them as raw bytes.
type RlpSerializableBytesTape struct {
	inner BytesTape
}

func NewRlpSerializableBytesTape(inner BytesTape) RlpSerializableTape {
	return &RlpSerializableBytesTape{inner}
}

func (t *RlpSerializableBytesTape) Next() (RlpSerializable, error) {
	value, err := t.inner.Next()
	if err != nil {
		return nil, err
	}

	return rlphacks.RlpSerializableBytes(value), nil
}

// RlpBytesTape is a wrapper on top of BytesTape that wraps every return value
// into RlpEncodedBytes, treating them as RLP-encode bytes.
// Hence, to get the double encoding we only need to encode once.
// Used when we know for sure that the data is already RLP encoded (accounts, tests, etc)
type RlpBytesTape struct {
	inner BytesTape
}

func NewRlpEncodedBytesTape(inner BytesTape) RlpSerializableTape {
	return &RlpBytesTape{inner}
}

func (t *RlpBytesTape) Next() (RlpSerializable, error) {
	value, err := t.inner.Next()
	if err != nil {
		return nil, err
	}

	return rlphacks.RlpEncodedBytes(value), nil
}
