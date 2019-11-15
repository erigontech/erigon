package trie

import (
	"io"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
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

// FIXME: comments here
type RlpSerializableTape interface {
	Next() (RlpSerializable, error)
}

type RlpSerializable interface {
	ToDoubleRLP(io.Writer) error
	DoubleRLPLen() int
	RawBytes() []byte
}

func NewRlpSerializableBytesTape(inner BytesTape) RlpSerializableTape {
	return &RlpSerializableBytesTape{inner}
}

type RlpSerializableBytesTape struct {
	inner BytesTape
}

func (t *RlpSerializableBytesTape) Next() (RlpSerializable, error) {
	value, err := t.inner.Next()
	if err != nil {
		return nil, err
	}

	return RlpSerializableBytes(value), nil
}

func NewRlpEncodedBytesTape(inner BytesTape) RlpSerializableTape {
	return &RlpBytesTape{inner}
}

type RlpBytesTape struct {
	inner BytesTape
}

func (t *RlpBytesTape) Next() (RlpSerializable, error) {
	value, err := t.inner.Next()
	if err != nil {
		return nil, err
	}

	return RlpEncodedBytes(value), nil
}
