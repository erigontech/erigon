package solid

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type ExtraData struct {
	data []byte

	l int
}

func NewExtraData() *ExtraData {
	return &ExtraData{
		data: make([]byte, 32),
	}
}

func (*ExtraData) Clone() clonable.Clonable {
	return NewExtraData()
}

func (*ExtraData) Static() bool {
	return false
}

func (e *ExtraData) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, e.Bytes()...), nil
}

func (e *ExtraData) EncodingSizeSSZ() int {
	return e.l
}

func (e *ExtraData) HashSSZ() ([32]byte, error) {
	leaves := make([]byte, length.Hash*2)
	copy(leaves, e.data[:e.l])
	binary.LittleEndian.PutUint64(leaves[length.Hash:], uint64(e.l))
	if err := merkle_tree.MerkleRootFromFlatLeaves(leaves, leaves); err != nil {
		return [32]byte{}, err
	}
	return common.BytesToHash(leaves[:length.Hash]), nil
}

func (e *ExtraData) DecodeSSZ(buf []byte, _ int) error {
	e.SetBytes(buf)
	return nil
}

func (e *ExtraData) Bytes() []byte {
	return common.Copy(e.data[:e.l])
}

func (e *ExtraData) SetBytes(buf []byte) {
	copy(e.data, buf)
	e.l = len(buf)
	if e.l > 32 {
		e.l = len(e.data)
	}
}
