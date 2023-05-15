package solid

import (
	"bytes"
	"encoding/binary"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
)

// BlockRoot 32
// Epoch 8
//
// total: 121
const checkpointSize = 32 + 8

type Checkpoint []byte

func NewCheckpointFromParameters(
	blockRoot libcommon.Hash,
	epoch uint64,
) Checkpoint {
	var c Checkpoint = make([]byte, checkpointSize)
	c.SetBlockRoot(blockRoot)
	c.SetEpoch(epoch)
	return c
}

func NewCheckpoint() Checkpoint {
	return make([]byte, checkpointSize)
}

func (c Checkpoint) SetBlockRoot(blockRoot libcommon.Hash) {
	copy(c[8:], blockRoot[:])
}

func (c Checkpoint) SetEpoch(epoch uint64) {
	binary.LittleEndian.PutUint64(c[:8], epoch)
}

func (c Checkpoint) Epoch() uint64 {
	return binary.LittleEndian.Uint64(c[:8])
}

func (c Checkpoint) BlockRoot() (o libcommon.Hash) {
	copy(o[:], c[8:])
	return
}

func (c Checkpoint) EncodingSizeSSZ() int {
	return 40
}

func (c Checkpoint) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < c.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	copy(c[:], buf)
	return nil
}

func (c Checkpoint) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, c[:]...)
	return buf, nil
}

func (c Checkpoint) Clone() clonable.Clonable {
	return NewCheckpoint()
}

func (c Checkpoint) Equal(other Checkpoint) bool {
	return bytes.Equal(c, other)
}

func (c Checkpoint) CopyHashBufferTo(o []byte) error {
	copy(o[:32], c[:8])
	copy(o[32:], c[8:])
	return nil
}

func (c Checkpoint) Copy() Checkpoint {
	o := NewCheckpoint()
	copy(o, c)
	return o
}

func (c Checkpoint) HashSSZ() (o [32]byte, err error) {
	leaves := make([]byte, 2*length.Hash)
	if err = c.CopyHashBufferTo(leaves); err != nil {
		return
	}
	err = TreeHashFlatSlice(leaves, o[:])
	return
}
