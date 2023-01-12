package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	ssz "github.com/prysmaticlabs/fastssz"

	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/common"
)

type Checkpoint struct {
	Epoch uint64
	Root  libcommon.Hash
}

func (c *Checkpoint) MarshalSSZTo(buf []byte) (dst []byte, err error) {
	dst = buf
	dst = ssz.MarshalUint64(dst, c.Epoch)
	dst = append(dst, c.Root[:]...)
	return
}

func (c *Checkpoint) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, 0, c.SizeSSZ())
	return c.MarshalSSZTo(buf)
}

func (c *Checkpoint) UnmarshalSSZ(buf []byte) error {
	var err error
	size := uint64(len(buf))
	if size != uint64(c.SizeSSZ()) {
		return ssz.ErrSize
	}

	// Field (0) 'Epoch'
	c.Epoch = ssz.UnmarshallUint64(buf[0:8])

	// Field (1) 'Root'
	copy(c.Root[:], buf[8:40])

	return err
}

func (c *Checkpoint) SizeSSZ() int {
	return common.BlockNumberLength + length.Hash
}

func (c *Checkpoint) HashTreeRoot() ([32]byte, error) {
	leaves := [][32]byte{
		merkle_tree.Uint64Root(c.Epoch),
		c.Root,
	}
	return merkle_tree.ArraysRoot(leaves, 2)
}

func (c *Checkpoint) HashTreeRootWith(hh *ssz.Hasher) (err error) {
	var root libcommon.Hash
	root, err = c.HashTreeRoot()
	if err != nil {
		return
	}

	hh.PutBytes(root[:])

	return
}
