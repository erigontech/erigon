package rlphacks

import (
	"io"
)

// RlpSerializableTape is an abstraction for a tape that allows reading a sequence of
// RlpSerializable values.
type RlpSerializableTape interface {
	Next() (RlpSerializable, error)
}

// RlpSerializable is a value that can be double-RLP coded.
type RlpSerializable interface {
	ToDoubleRLP(io.Writer, []byte) error
	DoubleRLPLen() int
	RawBytes() []byte
}
