package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	ssz "github.com/prysmaticlabs/fastssz"

	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

type Checkpoint struct {
	Epoch uint64
	Root  libcommon.Hash
}

func (c *Checkpoint) EncodeSSZ(buf []byte) []byte {
	return append(buf, append(ssz_utils.Uint64SSZ(c.Epoch), c.Root[:]...)...)
}

func (c *Checkpoint) DecodeSSZ(buf []byte) error {
	var err error
	size := uint64(len(buf))
	if size != uint64(c.EncodingSizeSSZ()) {
		return ssz.ErrSize
	}

	// Field (0) 'Epoch'
	c.Epoch = ssz.UnmarshallUint64(buf[0:8])

	// Field (1) 'Root'
	copy(c.Root[:], buf[8:40])

	return err
}

func (c *Checkpoint) EncodingSizeSSZ() int {
	return length.BlockNum + length.Hash
}

func (c *Checkpoint) HashSSZ() ([32]byte, error) {
	return merkle_tree.ArraysRoot([][32]byte{merkle_tree.Uint64Root(c.Epoch), c.Root}, 2)
}
