package solid

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

type IterableSSZ[T any] interface {
	Clear()
	CopyTo(IterableSSZ[T])
	Range(fn func(index int, value T, length int) bool)
	Get(index int) T
	Set(index int, v T)
	Length() int
	Cap() int

	Pop() T
	Append(v T)

	ssz2.Sized
	ssz.EncodableSSZ
	ssz.HashableSSZ
}

type Uint64VectorSSZ IterableSSZ[uint64]
type Uint64ListSSZ IterableSSZ[uint64]
type HashListSSZ IterableSSZ[common.Hash]
type HashVectorSSZ IterableSSZ[common.Hash]
