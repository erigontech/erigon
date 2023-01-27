package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"

	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type Checkpoint struct {
	Epoch uint64
	Root  libcommon.Hash
}

func (c *Checkpoint) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, append(ssz_utils.Uint64SSZ(c.Epoch), c.Root[:]...)...), nil
}

func (c *Checkpoint) DecodeSSZ(buf []byte) error {
	var err error
	size := uint64(len(buf))
	if size < uint64(c.EncodingSizeSSZ()) {
		return ssz_utils.ErrLowBufferSize
	}
	c.Epoch = ssz_utils.UnmarshalUint64SSZ(buf[0:8])
	copy(c.Root[:], buf[8:40])

	return err
}

func (c *Checkpoint) EncodingSizeSSZ() int {
	return length.BlockNum + length.Hash
}

func (c *Checkpoint) HashSSZ() ([32]byte, error) {
	return merkle_tree.ArraysRoot([][32]byte{merkle_tree.Uint64Root(c.Epoch), c.Root}, 2)
}
