package solid

import (
	"bytes"
	"encoding/binary"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

// Constants to represent the size and layout of a Checkpoint
const checkpointSize = 32 + 8 // BlockRoot(32 bytes) + Epoch(8 bytes)

type Checkpoint []byte // Define Checkpoint as a byte slice

// NewCheckpointFromParameters returns a new Checkpoint constructed from the given blockRoot and epoch
func NewCheckpointFromParameters(
	blockRoot libcommon.Hash, // A hash representing the block root
	epoch uint64, // An unsigned 64-bit integer representing the epoch
) Checkpoint {
	var c Checkpoint = make([]byte, checkpointSize)
	c.SetBlockRoot(blockRoot)
	c.SetEpoch(epoch)
	return c
}

// NewCheckpoint returns a new Checkpoint with the underlying byte slice initialized to zeros
func NewCheckpoint() Checkpoint {
	return make([]byte, checkpointSize)
}

// SetBlockRoot copies the given blockRoot into the correct location within the Checkpoint
func (c Checkpoint) SetBlockRoot(blockRoot libcommon.Hash) {
	copy(c[8:], blockRoot[:]) // copy the blockRoot into the Checkpoint starting at index 8
}

// SetEpoch encodes the given epoch into the correct location within the Checkpoint
func (c Checkpoint) SetEpoch(epoch uint64) {
	binary.LittleEndian.PutUint64(c[:8], epoch) // encode the epoch into the first 8 bytes of the Checkpoint
}

// Epoch returns the epoch encoded within the Checkpoint
func (c Checkpoint) Epoch() uint64 {
	return binary.LittleEndian.Uint64(c[:8]) // decode and return the epoch from the first 8 bytes of the Checkpoint
}

// BlockRoot returns the block root encoded within the Checkpoint
func (c Checkpoint) BlockRoot() (o libcommon.Hash) {
	copy(o[:], c[8:]) // copy and return the block root from the Checkpoint starting at index 8
	return
}

// EncodingSizeSSZ returns the size of the Checkpoint object when encoded as SSZ.
func (Checkpoint) EncodingSizeSSZ() int {
	return checkpointSize
}

// DecodeSSZ decodes the Checkpoint object from SSZ-encoded data.
func (c Checkpoint) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < c.EncodingSizeSSZ() {
		// If the buffer size is smaller than the required size of the Checkpoint, return an error.
		return ssz.ErrLowBufferSize
	}
	copy(c[:], buf)
	return nil
}

// EncodeSSZ encodes the Checkpoint object into SSZ format.
func (c Checkpoint) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, c[:]...), nil
}

// Clone returns a new Checkpoint object that is a copy of the current object.
func (c Checkpoint) Clone() clonable.Clonable {
	return NewCheckpoint()
}

// Equal checks if the Checkpoint object is equal to another Checkpoint object.
func (c Checkpoint) Equal(other Checkpoint) bool {
	return bytes.Equal(c, other)
}

// CopyHashBufferTo copies the hash of the Checkpoint to the buffer 'o'.
func (c Checkpoint) CopyHashBufferTo(o []byte) error {
	copy(o[:32], c[:8])
	copy(o[32:], c[8:])
	return nil
}

// Copy returns a copy of the Checkpoint object.
func (c Checkpoint) Copy() Checkpoint {
	o := NewCheckpoint()
	copy(o, c)
	return o
}

// HashSSZ returns the hash of the Checkpoint object when encoded as SSZ.
func (c Checkpoint) HashSSZ() (o [32]byte, err error) {
	leaves := make([]byte, 2*length.Hash)
	if err = c.CopyHashBufferTo(leaves); err != nil {
		return
	}
	err = merkle_tree.MerkleRootFromFlatLeaves(leaves, o[:])
	return
}

// Static always returns true, indicating that the Checkpoint object is static.
func (c Checkpoint) Static() bool {
	return true
}
