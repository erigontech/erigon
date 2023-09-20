package solid

import (
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/types/clonable"
)

type uint64ListSSZ struct {
	u *byteBasedUint64Slice
}

func NewUint64ListSSZ(limit int) Uint64ListSSZ {
	return &uint64ListSSZ{
		u: NewUint64Slice(limit),
	}
}

func (h uint64ListSSZ) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.u)
}

func (h uint64ListSSZ) UnmarshalJSON(buf []byte) error {
	return json.Unmarshal(buf, h.u)
}

func (h *uint64ListSSZ) Static() bool {
	return false
}

func NewUint64ListSSZFromSlice(limit int, slice []uint64) Uint64ListSSZ {
	x := &uint64ListSSZ{
		u: NewUint64Slice(limit),
	}
	for _, num := range slice {
		x.Append(num)
	}
	return x
}

func (arr *uint64ListSSZ) Clear() {
	arr.u.Clear()
}

func (arr *uint64ListSSZ) CopyTo(target IterableSSZ[uint64]) {
	if c, ok := target.(*uint64ListSSZ); ok {
		arr.u.CopyTo(c.u)
		return
	}
	panic("incompatible type")
}

func (arr *uint64ListSSZ) Range(fn func(index int, value uint64, length int) bool) {
	arr.u.Range(fn)
}

func (arr *uint64ListSSZ) Get(index int) uint64 {
	return arr.u.Get(index)
}

func (arr *uint64ListSSZ) Set(index int, v uint64) {
	arr.u.Set(index, v)
}

func (arr *uint64ListSSZ) Length() int {
	return arr.u.Length()
}

func (arr *uint64ListSSZ) Cap() int {
	return arr.u.Cap()
}

func (arr *uint64ListSSZ) HashSSZ() ([32]byte, error) {
	return arr.u.HashListSSZ()
}

func (arr *uint64ListSSZ) Clone() clonable.Clonable {
	return NewUint64ListSSZ(arr.Cap())
}

func (arr *uint64ListSSZ) EncodeSSZ(buf []byte) (dst []byte, err error) {
	return arr.u.EncodeSSZ(buf)
}

func (arr *uint64ListSSZ) DecodeSSZ(buf []byte, version int) error {
	return arr.u.DecodeSSZ(buf, version)
}

func (arr *uint64ListSSZ) EncodingSizeSSZ() int {
	return arr.u.EncodingSizeSSZ()
}

func (arr *uint64ListSSZ) Pop() uint64 {
	return arr.u.Pop()
}

func (arr *uint64ListSSZ) Append(v uint64) {
	arr.u.Append(v)
}

// Check if it is sorted and check if there are duplicates. O(N) complexity.
func IsUint64SortedSet(set IterableSSZ[uint64]) bool {
	for i := 0; i < set.Length()-1; i++ {
		if set.Get(i) >= set.Get(i+1) {
			return false
		}
	}
	return true
}

func IntersectionOfSortedSets(v1, v2 IterableSSZ[uint64]) []uint64 {
	intersection := []uint64{}
	// keep track of v1 and v2 element iteration
	var i, j int
	// Note that v1 and v2 are both sorted.
	for i < v1.Length() && j < v2.Length() {
		if v1.Get(i) == v2.Get(j) {
			intersection = append(intersection, v1.Get(i))
			// Change both iterators
			i++
			j++
			continue
		}
		// increase i and j accordingly
		if v1.Get(i) > v2.Get(j) {
			j++
		} else {
			i++
		}
	}
	return intersection
}
