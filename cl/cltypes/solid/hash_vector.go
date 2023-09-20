package solid

import (
	"encoding/json"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type hashVector struct {
	u *hashList
}

func NewHashVector(s int) HashVectorSSZ {
	return &hashVector{
		u: &hashList{
			u: make([]byte, s*length.Hash),
			c: int(merkle_tree.NextPowerOfTwo(uint64(s))),
			l: s,
		},
	}
}

func (h *hashVector) Append(val libcommon.Hash) {
	panic("not implmented")
}

func (h hashVector) MarshalJSON() ([]byte, error) {
	return json.Marshal(h.u)
}

func (h *hashVector) UnmarshalJSON(buf []byte) error {
	return json.Unmarshal(buf, h.u)
}

func (h *hashVector) Cap() int {
	return h.u.l
}

func (h *hashVector) Length() int {
	return h.u.l
}

func (h *hashVector) Clear() {
	panic("not implemented")
}

func (h *hashVector) Clone() clonable.Clonable {
	return NewHashVector(h.u.l)
}

func (h *hashVector) CopyTo(t IterableSSZ[libcommon.Hash]) {
	tu := t.(*hashVector)
	h.u.CopyTo(tu.u)
}

func (h *hashVector) Static() bool {
	return true
}

func (h *hashVector) DecodeSSZ(buf []byte, version int) error {
	if len(buf) < h.Length()*length.Hash {
		return ssz.ErrBadDynamicLength
	}
	copy(h.u.u, buf)
	return nil
}

func (h *hashVector) EncodeSSZ(buf []byte) ([]byte, error) {
	return h.u.EncodeSSZ(buf)
}

func (h *hashVector) EncodingSizeSSZ() int {
	return h.u.EncodingSizeSSZ()
}

func (h *hashVector) Get(index int) (out libcommon.Hash) {
	return h.u.Get(index)
}

func (h *hashVector) Set(index int, newValue libcommon.Hash) {
	h.u.Set(index, newValue)
}

func (h *hashVector) HashSSZ() ([32]byte, error) {
	return h.u.hashVectorSSZ()
}

func (h *hashVector) Range(fn func(int, libcommon.Hash, int) bool) {
	h.u.Range(fn)
}

func (h *hashVector) Pop() libcommon.Hash {
	panic("didnt ask, dont need it, go fuck yourself")
}
