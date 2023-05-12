package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type Checkpoint struct {
	Epoch uint64
	Root  libcommon.Hash
}

func (c *Checkpoint) Copy() *Checkpoint {
	copiedCheckpoint := new(Checkpoint)
	*copiedCheckpoint = *c
	return copiedCheckpoint
}

func (c *Checkpoint) Equal(other *Checkpoint) bool {
	return c.Epoch == other.Epoch && c.Root == other.Root
}

func (c *Checkpoint) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, append(ssz.Uint64SSZ(c.Epoch), c.Root[:]...)...), nil
}

func (c *Checkpoint) DecodeSSZ(buf []byte, _ int) error {
	var err error
	size := uint64(len(buf))
	if size < uint64(c.EncodingSizeSSZ()) {
		return fmt.Errorf("[Checkpoint] err: %s", ssz.ErrLowBufferSize)
	}
	c.Epoch = ssz.UnmarshalUint64SSZ(buf[0:8])
	copy(c.Root[:], buf[8:40])

	return err
}

func (c *Checkpoint) EncodingSizeSSZ() int {
	return length.BlockNum + length.Hash
}

func (c *Checkpoint) HashSSZ() ([32]byte, error) {
	return merkle_tree.ArraysRoot([][32]byte{merkle_tree.Uint64Root(c.Epoch), c.Root}, 2)
}
