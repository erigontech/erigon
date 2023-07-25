package binaryblob

import (
	"context"
	"fmt"
	"io/fs"
	"math"
	"strconv"

	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/spf13/afero"
)

type Case struct {
	CheckpointFs afero.Fs
	DominoFs     afero.Fs

	GenesisConfig *clparams.GenesisConfig
	BeaconConfig  *clparams.BeaconChainConfig
}

func (c *Case) stupidSeek(slot uint64) (fs.FileInfo, error) {
	files, err := afero.ReadDir(c.CheckpointFs, "")
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no checkpoints")
	}
	file := files[0]
	diff := math.MaxInt
	for _, v := range files {
		i, err := strconv.Atoi(v.Name())
		if err != nil {
			continue
		}
		if uint64(i) > slot {
			break
		}
		newDiff := int(slot) - i
		if newDiff < diff {
			file = v
		}
	}
	return file, nil
}

// Checkpoint should get the state at or before the slot selected
func (c *Case) Checkpoint(ctx context.Context, slot uint64) (abstract.BeaconState, error) {
	fi, err := c.stupidSeek(slot)
	if err != nil {
		return nil, err
	}
	bts, err := afero.ReadFile(c.CheckpointFs, fmt.Sprintf("%s/data.bin", fi.Name()))
	if err != nil {
		return nil, err
	}
	_, version := c.EpochAndVersion()
	beaconState := state.New(c.BeaconConfig)
	err = beaconState.DecodeSSZ(bts, version)
	if err != nil {
		return nil, err
	}
	return beaconState, nil
}

// Dominos should get a block
func (c *Case) Domino(ctx context.Context, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	bts, err := afero.ReadFile(c.DominoFs, fmt.Sprintf("%d/data.bin", slot))
	if err != nil {
		return nil, err
	}
	_, version := c.EpochAndVersion()
	blk := &cltypes.SignedBeaconBlock{}
	err = blk.DecodeSSZ(bts, version)
	if err != nil {
		return nil, err
	}
	return blk, nil
}

func (c *Case) EpochAndVersion() (epoch uint64, version int) {
	epoch = utils.GetCurrentEpoch(c.GenesisConfig.GenesisTime, c.BeaconConfig.SecondsPerSlot, c.BeaconConfig.SlotsPerEpoch)
	version = int(c.BeaconConfig.GetCurrentStateVersion(epoch))
	return

}
