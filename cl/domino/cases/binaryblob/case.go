package binaryblob

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/spf13/afero"
)

type Case struct {
	CheckpointFs afero.Fs
	DominoFs     afero.Fs

	GenesisConfig *clparams.GenesisConfig
	BeaconConfig  *clparams.BeaconChainConfig
}

// Checkpoint should get the state at or before the slot selected
func (c *Case) Checkpoint(ctx context.Context, slot uint64) (abstract.BeaconState, error) {
	panic("not implemented") // TODO: Implement
}

// Dominos should get a block
func (c *Case) Domino(ctx context.Context, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	bts, err := afero.ReadFile(c.DominoFs, block)
	if err != nil {
		return nil, err
	}
	blk := &cltypes.SignedBeaconBlock{}
	err = blk.DecodeSSZ(bts, 0)
	if err != nil {
		return nil, err
	}
	panic("not implemented") // TODO: Implement
}
